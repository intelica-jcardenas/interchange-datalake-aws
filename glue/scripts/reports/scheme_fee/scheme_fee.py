"""
glue-vi-mc-scheme-fee (job real: itl-0004-itx-dev-intchg-02-glue-scheme-fee,
todavia no creado en AWS -- ver pending.md "Scheme Fee")
=========================================================================
Replica del modulo legacy "Scheme Fee" (Module.SchemeFee.managment.scheme_fee,
ejecutado antes en un EC2 via exec_scheme_fee.py) sobre el nuevo pipeline
Data Lake. Genera y actualiza el reporte mensual de cuotas (scheme fees) que
se intercambia como CSV con el equipo externo que calcula el costo real.

Referencia legacy (no forma parte de este repo, sólo contexto): 3 scripts
subidos por el usuario a tst_files/scheme_Fee_legacy_scripts/ (exec_scheme_fee.py,
managment.py, getquery.py). Ese proceso corría contra PostgreSQL (tablas
operational.mh_transaction_scheme_fee / mh_monthly_scheme_fee /
mh_monthly_scheme_fee_legacy / mh_scheme_fee_sumary) usando un EC2. Este script
reimplementa la MISMA lógica de negocio en PySpark sobre S3, sin base de datos:
el "estado" que el legacy guardaba en tablas Postgres aquí se guarda como
Parquet en el bucket analytics.

Proceso en 2 pasos (igual que legacy, ahora como --mode del job):

  --mode generate
    1. Lee Visa BASEII y Mastercard IPM_1240/1442 operational del mes pedido.
       (Visa SMS queda deshabilitado por defecto - ver ENABLE_SMS más abajo,
       mismo estado "pendiente validación" que en glue-vi-mc-reporting.)
    2. Arma el detalle transaccional (~20 dimensiones de negocio + monto en
       report_currency), aplica duplicado on-us cuando el cliente lo requiere
       (duplicate_on_us_flag_visa / _mastercard en DynamoDB client-02).
    3. Agrupa el detalle en filas de "reporte" (una fila por combinación única
       de las 20 dimensiones — el mismo grano que usaba el SP legacy).
    4. Exporta el reporte en el formato CSV legacy exacto (columnas
       abreviadas: rpt_bnk_id, set_mth, bus_id, sch_id, tkt_siz_id, prd_id,
       prg_id, fnd_src_id, txn_scp_id, txn_typ_id, txn_rvsl_flg_id,
       txn_crncy_lcl_flg_id, txn_crd_prs_flg_id, mct_cd, txn_cnt, txn_amt,
       txn_sfc, mct_ctry_id, unt_sfc, est_sch_fee_amt, unt_est_sch_fee_amt,
       swt_cd, trvl_prg_ind, grc_mpymt_ind, key_etrd_tpe) a
       s3://{scheme_fee_bucket}/OUT/{client}/{client}_{report_month}_{ts}.csv
    5. Persiste el estado (detalle + reporte + summary) en el bucket
       analytics, necesario para el paso --mode read posterior.

  --mode read
    1. Lee el CSV que el equipo externo devolvió (con txn_sfc / est_sch_fee_amt
       ya completados) desde s3://{scheme_fee_bucket}/IN/{client}/{in_file_key}
    2. Valida que la cantidad de filas coincida con el summary generado en el
       paso anterior (mismo chequeo de seguridad que hacía el legacy).
    3. Calcula unt_sfc = txn_sfc / txn_cnt y unt_est_sch_fee_amt =
       est_sch_fee_amt / txn_cnt (igual que legacy, no vienen en el CSV).
    4. Actualiza el reporte (join por app_id, igual que legacy — el equipo
       externo devuelve el mismo app_id que se le entregó, no se re-matchea
       por las 20 dimensiones).
    5. Propaga txn_sfc/unt_sfc/est_sch_fee_amt/unt_est_sch_fee_amt al detalle
       transaccional (join por las 20 dimensiones — igual que legacy
       get_update_detail), dejando el detalle final listo para ser consumido
       por scheme_fees_amount en glue-vi-mc-reporting (get_transaction.py)
       usando la columna UNITARIA (unt_sfc), no transaction_scheme_fee_cost
       (que replica el TOTAL del grupo en cada fila, igual que hacía legacy —
       ver nota en update_report_and_propagate()).

Fuentes de datos
----------------
  S3 operational  : Visa baseii_drafts, MC IPM_1240, MC IPM_1442
  S3 reference    : country/data.parquet, currency/data.parquet,
                    exchange-rates-glue/brand=*/exchange_date=*/, visa_bin_products/data.parquet,
                    mastercard_bin_products/data.parquet
                    + 2 tablas NUEVAS que no existen aún en s3-reference
                    (ver REFERENCIA NUEVA REQUERIDA más abajo)
  DynamoDB        : tabla client (report_currency_code, local_currency_code,
                    customer_country, duplicate_on_us_flag_visa/_mastercard)

Salida
------
  CSV (intercambio con equipo externo):
    s3://{scheme_fee_bucket}/OUT/{client}/{client}_{report_month}_{ts}.csv   (generate)
    s3://{scheme_fee_bucket}/IN/{client}/{in_file_key}                       (leído en read)
  Estado interno (Parquet, reemplaza las tablas Postgres del legacy):
    s3://{analytics_bucket}/{client}/scheme_fee/state/{report_month}/detail/
    s3://{analytics_bucket}/{client}/scheme_fee/state/{report_month}/report/
    s3://{analytics_bucket}/{client}/scheme_fee/state/{report_month}/summary.json
    s3://{analytics_bucket}/{client}/scheme_fee/final/{report_month}/detail/   (tras read)
    s3://{analytics_bucket}/{client}/scheme_fee/final/{report_month}/report/   (tras read)

REFERENCIA NUEVA REQUERIDA (no existe aún en s3-reference — subir antes de
correr el job; ver detalle de columnas en load_bin_funding_source(),
load_size_ticket() y load_scheme_fee_bin_products() más abajo):
  1. s3://{reference_bucket}/bin_funding_source/data.parquet
     cols: bin_funding_source_code (string, 'C'/'D'), bin_funding_source_mc_numeric_code (int)
     Equivalente legacy: operational.m_bin_funding_source
  2. s3://{reference_bucket}/size_ticket/data.parquet
     cols: brand (string, 'VISA'/'MasterCard'), country_code_list (string, alpha-3),
           ticket_currency (string, alpha-3), size_ticket_min (double),
           size_ticket_max (double), app_id (int — id del bucket de ticket size)
     Equivalente legacy: operational.m_size_ticket
  3. s3://{reference_bucket}/scheme_fee_bin_products/data.parquet
     cols: product_code (string), range_program_id (int), legacy_product_id (int),
           brand (string, 'VISA'/'MC' — OJO, distinto de size_ticket que usa 'MasterCard')
     Equivalente legacy: operational.m_scheme_fee_bin_products
     IMPORTANTE: NO reusar visa_bin_products/mastercard_bin_products (las que
     ya usa get_transaction.py) para esto — están desalineadas de
     m_scheme_fee_bin_products (2 product_code de VISA y 17 de Mastercard con
     range_program_id distinto, más cobertura de filas distinta). El legacy
     de scheme fee siempre usó m_scheme_fee_bin_products específicamente.

Requiere además el argumento de job --dynamodb_table_file_control (tabla
file_control, usada por switch_code — ver get_local_switch_content_hashes).

SIMPLIFICACIONES CONOCIDAS respecto al legacy (documentadas, no bloqueantes):
  - Visa SMS: implementado pero deshabilitado por defecto (ENABLE_SMS=False),
    mismo estado "pendiente validación end-to-end" que transform_visa_sms en
    glue-vi-mc-reporting.

Parámetros del job
------------------
  --mode                  "generate" | "read"
  --client_code           Código de cliente único, ej: "EBGR"
  --report_month          Mes del reporte en formato YYYYMM, ej: "202601"
  --scheme_fee_bucket     Bucket de intercambio OUT/IN con el equipo externo
  --operational_bucket    Bucket operational (lectura de Parquets)
  --reference_bucket      Bucket reference (country, currency, bin products, exchange_rate)
  --analytics_bucket      Bucket analytics (estado interno + reportes)
  --dynamodb_table_client Tabla DynamoDB de clientes
  --force                 "true"/"false" — sólo en generate. Si ya existe un
                           summary previo para el mismo client+report_month y
                           force=false, el job aborta (igual que la
                           confirmación interactiva del legacy, que en un job
                           batch no puede pedirse).
  --in_file_key           Sólo en read. Nombre del archivo bajo
                           IN/{client_code}/ en scheme_fee_bucket.
"""

import sys
import re
import json
from datetime import datetime
from calendar import monthrange

import boto3
import pandas as pd

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DateType,
)
from pyspark.sql import SparkSession, DataFrame

# =============================================================================
# Spark / Glue setup
# =============================================================================

spark = (
    SparkSession.builder.config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    .getOrCreate()
)
glueContext = GlueContext(spark.sparkContext)
logger = glueContext.get_logger()


def log_info(message: str):
    logger.info(f"GlueLogger: {message}")


def log_error(message: str):
    logger.error(f"GlueLogger: {message}")


# =============================================================================
# PARAMETROS DEL JOB
# =============================================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "mode",
        "client_code",
        "report_month",
        "scheme_fee_bucket",
        "operational_bucket",
        "reference_bucket",
        "analytics_bucket",
        "dynamodb_table_client",
        "dynamodb_table_file_control",
        "force",
        "in_file_key",
    ],
)

MODE = args["mode"].strip().lower()
CLIENT_CODE = args["client_code"].strip().upper()
REPORT_MONTH = args["report_month"].strip()
SCHEME_FEE_BUCKET = args["scheme_fee_bucket"]
OPERATIONAL_BUCKET = args["operational_bucket"]
BUCKET_REF = args["reference_bucket"]
ANALYTICS_BUCKET = args["analytics_bucket"]
DDB_CLIENT_TABLE = args["dynamodb_table_client"]
DDB_FILE_CONTROL_TABLE = args["dynamodb_table_file_control"]
FORCE = args["force"].strip().lower() == "true"
IN_FILE_KEY = args["in_file_key"].strip()

ENABLE_SMS = True

s3 = boto3.client("s3")

STATE_PREFIX = f"{CLIENT_CODE}/scheme_fee/state/{REPORT_MONTH}"
FINAL_PREFIX = f"{CLIENT_CODE}/scheme_fee/final/{REPORT_MONTH}"

# =============================================================================
# CONSTANTES DE NEGOCIO (portadas literalmente del legacy — no son "datos de
# referencia" externos, son reglas fijas embebidas en el SQL original)
# =============================================================================

# Países EU usados en key_entered_tpe (Mastercard, "key entered" intraregional)
GREECE_EU_COUNTRIES = [
    "ALA", "AUT", "BEL", "BGR", "HRV", "CZE", "CYP", "DNK", "EST", "FIN",
    "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ITA", "LVA", "LIE", "LTU",
    "LUX", "MLT", "NLD", "NOR", "POL", "PRT", "ROU", "SVK", "SVN", "ESP",
    "SWE", "GUF", "GLP", "MTQ", "REU", "MAF", "MYT", "GBR",
]

# Merchant category codes usados en greece_micropayment_indicator (Visa)
GREECE_MICROPAYMENT_MCCS = [
    4121, 5192, 5331, 5399, 5411, 5441, 5451, 5462, 5499, 5697,
    5949, 5993, 5994, 7216, 7251,
]
GREECE_MICROPAYMENT_PRODUCT_IDS = ["C", "F", "F2", "P", "N", "I", "L", "N2"]

# Mastercard gcms_product_identifier usados en travel_program_indicator
MC_TRAVEL_PRODUCT_IDS_A = [
    "MTA", "MTB", "MTC", "MTD", "MTE", "MTF", "MTG", "MTH", "MTI", "MTJ",
    "MTK", "MTL", "MTM", "MTN", "MTO", "MTQ", "MTR", "MTS", "MTT", "MTU", "MTV",
]
MC_TRAVEL_PRODUCT_IDS_B = ["MBS", "MBA", "MBG", "MBH", "MBI", "MBJ"]
MC_TRAVEL_MCC_RANGE = (3000, 3999)
MC_TRAVEL_MCC_LIST = [4511, 4411, 4131, 4582, 4722, 5962, 6513, 7012, 7032,
                       7033, 7298, 7991, 7997, 7999, 7011, 4112, 7512, 7513, 7519]

# Las 20 dimensiones de agrupación del reporte (mismo grano que
# get_insert_into_report_table del legacy). Orden preservado por legibilidad,
# no es funcionalmente relevante para el group by.
GROUP_DIMS = [
    "report_currency", "app_customer_code", "bank_country", "business_mode_id",
    "transaction_brand", "size_ticket", "product_id", "range_program_id",
    "account_funding_source", "business_transaction_type_id", "jurisdiction",
    "reversal_indicator", "currency_local_indicator", "motoec_indicator",
    "account_number", "merchant_country_code", "switch_code",
    "travel_program_indicator", "greece_micropayment_indicator", "key_entered_tpe",
]


# =============================================================================
# HELPERS COMPARTIDOS (mismo patrón que glue-vi-mc-reporting/get_transaction.py)
# =============================================================================


def get_client_config(client_code: str) -> dict:
    """
    Lee configuración del cliente desde DynamoDB.

    A diferencia de get_client_config() en get_transaction.py (que hace
    bool(item.get(...)) — con boto3.resource los valores llegan como string,
    y bool("FALSE") es True en Python, lo cual castea cualquier flag no vacío
    a True sin importar su contenido real), acá se parsea explícitamente
    comparando contra "TRUE".
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DDB_CLIENT_TABLE)
    resp = table.get_item(Key={"client_id": client_code})
    item = resp.get("Item", {})

    def _flag(value) -> bool:
        return str(value).strip().upper() == "TRUE"

    return {
        "report_currency": item.get("report_currency_code") or "USD",
        "local_currency_code": item.get("local_currency_code") or "",
        "customer_country": item.get("customer_country") or "",
        "dup_on_us_visa": _flag(item.get("duplicate_on_us_flag_visa", "")),
        "dup_on_us_mc": _flag(item.get("duplicate_on_us_flag_mastercard", "")),
    }


_LOCAL_FILE_PATTERN = re.compile(r"local_visa|local_mastercard", re.IGNORECASE)


def get_local_switch_content_hashes(client_code: str) -> set:
    """
    Replica update_switch_codes() del legacy (getquery.py lineas 1198-1244):
    switch_code solo se aplica a archivos cuyo process_file_name matcheaba
    'Local_VISA_%'/'Local_MasterCard_%' -- ese dato (nombre original del
    archivo) NO existe en operational, solo en DynamoDB file_control
    (campo landing_file_name).

    Se resuelve con un scan filtrado por client_id (barato, el total de la
    tabla es ~600 items) en vez de una consulta por fila. Devuelve el set de
    content_hash (NO file_id) de los archivos que matchean el patron -- el
    join debe ser por content_hash porque el nombre del Parquet operational
    (lo que read_operational() usa) es el content_hash, y content_hash/file_id
    pueden diferir tras un reproceso (ver gotcha en gotchas.md).
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DDB_FILE_CONTROL_TABLE)

    hashes = set()
    scan_kwargs = {
        "FilterExpression": "client_id = :c",
        "ExpressionAttributeValues": {":c": client_code},
        "ProjectionExpression": "content_hash, landing_file_name",
    }
    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            name = item.get("landing_file_name") or ""
            content_hash = item.get("content_hash")
            if content_hash and _LOCAL_FILE_PATTERN.search(name):
                hashes.add(content_hash)
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    log_info(f"[get_local_switch_content_hashes] {client_code}: {len(hashes)} archivos 'Local_' encontrados")
    return hashes


def load_local_switch() -> DataFrame:
    """
    NUEVA tabla de referencia -- equivalente legacy operational.m_local_switch.
    Legacy: update_switch_codes() (getquery.py linea 1235), join por
    brand_code = transaction_brand AND customer_code = app_customer_code.

    cols esperadas: customer_code (str), brand_code (str, 'VISA'/'MasterCard'
    -- coincide con transaction_brand), switch_id (int), switch_code (str),
    switch_desc (str).
    """
    path = f"s3://{BUCKET_REF}/local_switch/data.parquet"
    try:
        return spark.read.parquet(path).select(
            "customer_code",
            "brand_code",
            F.col("switch_id").cast(IntegerType()),
        )
    except Exception as e:
        log_error(f"[load_local_switch] No encontrada en {path}: {e} — switch_code saldra siempre en 0")
        return spark.createDataFrame([], schema="customer_code string, brand_code string, switch_id int")


def _resolve_switch_code(
    df: DataFrame, local_switch_df: DataFrame, local_hashes: set, customer_code: str, brand_literal: str
) -> DataFrame:
    """
    Aplica switch_code = switch_id solo a filas cuyo content_hash está en
    local_hashes (archivos 'Local_VISA_'/'Local_MasterCard_', resuelto via
    get_local_switch_content_hashes) Y cuyo (customer_code, brand) matchea en
    local_switch_df. local_switch es una tabla chica (2 filas hoy, solo
    SBSA) -- se resuelve el switch_id de (customer_code, brand_literal) una
    sola vez con collect(), en vez de un join Spark completo.
    """
    switch_id = 0
    row = (
        local_switch_df.filter(
            (F.col("customer_code") == customer_code) & (F.col("brand_code") == brand_literal)
        )
        .select("switch_id")
        .limit(1)
        .collect()
    )
    if row:
        switch_id = row[0]["switch_id"] or 0

    is_local = F.col("content_hash").isin(*local_hashes) if local_hashes else F.lit(False)
    return df.withColumn("switch_code", F.when(is_local, F.lit(switch_id)).otherwise(F.lit(0)))


def read_operational(
    client_id: str, brand: str, data_type: str, start_date: str, end_date: str
) -> DataFrame | None:
    """
    Basado en read_operational() de get_transaction.py, con una corrección:
    esa versión derivaba una columna "file_id" con
    regexp_extract(input_file_name(), ...) — pero el nombre del Parquet
    operational es literalmente el content_hash (ver lmbd-vi-store/
    lmbd-mc-store, escriben "{content_hash}.parquet"), así que esa columna
    quedaba con el MISMO valor que la columna real "content_hash" ya presente
    en el Parquet, bajo un nombre ambiguo que sugiere ser el file_id de
    DynamoDB (que puede ser distinto tras un reproceso — ver gotcha
    file_id-vs-content_hash en gotchas.md). Acá se usa directamente la
    columna "content_hash" original, sin derivar un duplicado — es la llave
    que se necesitará más adelante para cruzar contra get_transaction.py y
    devolver el costo de scheme fee calculado a nivel transacción.
    """
    base_path = f"s3://{OPERATIONAL_BUCKET}/{client_id}/{brand}/{data_type}/"
    try:
        df = spark.read.parquet(base_path)
    except Exception as e:
        log_info(f"[read_operational] No data at {base_path}: {e}")
        return None

    df = df.filter(F.col("date").between(start_date, end_date))
    if df.rdd.isEmpty():
        log_info(f"[read_operational] No rows for {client_id}/{brand}/{data_type} in [{start_date}, {end_date}]")
        return None

    df = df.withColumn("customer_code", F.lit(client_id))
    log_info(f"[read_operational] Loaded {client_id}/{brand}/{data_type}: {df.count()} rows")
    return df


def load_country() -> DataFrame:
    """country_code (2-letter), country_code_alternative (3-letter), legacy_country_id."""
    path = f"s3://{BUCKET_REF}/country/data.parquet"
    return spark.read.parquet(path).select(
        "country_code", "country_code_alternative", "legacy_country_id"
    )


def load_currency() -> DataFrame:
    path = f"s3://{BUCKET_REF}/currency/data.parquet"
    return spark.read.parquet(path).select("currency_numeric_code", "currency_alphabetic_code")


def load_scheme_fee_bin_products() -> DataFrame:
    """
    NUEVA tabla de referencia — equivalente legacy operational.m_scheme_fee_bin_products.

    NO reusar visa_bin_products/mastercard_bin_products (las que usa
    get_transaction.py): aunque mapean el mismo product_code -> range_program_id,
    están desalineadas de m_scheme_fee_bin_products (2 product_code de VISA y
    17 de Mastercard con range_program_id distinto entre ambas fuentes, y
    cobertura de filas distinta). El legacy de scheme fee siempre usó
    m_scheme_fee_bin_products, nunca las otras dos tablas.

    cols esperadas: product_code (str), range_program_id (int),
    legacy_product_id (int), brand (str — valores reales 'VISA'/'MC', NO
    'MasterCard' como en size_ticket — normalizar contra brand_code
    explícito al filtrar, no contra transaction_brand).
    """
    path = f"s3://{BUCKET_REF}/scheme_fee_bin_products/data.parquet"
    try:
        return spark.read.parquet(path).select(
            F.trim(F.col("product_code")).alias("product_code"),
            F.col("range_program_id").cast(IntegerType()).alias("range_program_id"),
            F.col("legacy_product_id").cast(IntegerType()).alias("legacy_product_id"),
            F.upper(F.trim(F.col("brand"))).alias("brand"),
        )
    except Exception as e:
        log_error(f"[load_scheme_fee_bin_products] No encontrada en {path}: {e} — range_program_id/legacy_product_id saldrán null (prg_id/prd_id con sentinel 255)")
        return spark.createDataFrame(
            [], schema="product_code string, range_program_id int, legacy_product_id int, brand string"
        )


def _product_ref_for_brand(scheme_fee_bin_products_df: DataFrame, brand_code: str) -> DataFrame:
    """product_ref filtrado a un solo brand ('VISA' o 'MC', valores reales de la tabla)."""
    return (
        scheme_fee_bin_products_df.filter(F.col("brand") == brand_code)
        .select(
            F.col("product_code").alias("_pc"),
            F.col("range_program_id"),
            F.col("legacy_product_id"),
        )
    )


def load_bin_funding_source() -> DataFrame:
    """
    NUEVA tabla de referencia (ver docstring del módulo).
    cols: bin_funding_source_code (string), bin_funding_source_mc_numeric_code (int)
    """
    path = f"s3://{BUCKET_REF}/bin_funding_source/data.parquet"
    try:
        return spark.read.parquet(path).select(
            "bin_funding_source_code", "bin_funding_source_mc_numeric_code"
        )
    except Exception as e:
        log_error(f"[load_bin_funding_source] No encontrada en {path}: {e} — fnd_src_id saldrá con sentinel 255")
        return spark.createDataFrame(
            [], schema="bin_funding_source_code string, bin_funding_source_mc_numeric_code int"
        )


def load_visa_business_transaction_type() -> DataFrame:
    """
    NUEVA tabla de referencia -- equivalente legacy operational.m_visa_business_transaction_type.
    Legacy: left join ... on cast(business_transaction_type_id as integer) = cf.business_transaction_type
    (getquery.py get_issuers_visa/get_acquirer_visa/get_sms_visa, lineas 354/456/566/659/762).

    cols esperadas: business_transaction_type_id (int, PK -- mismo valor que
    cf.business_transaction_type calculado en calc_business_transaction_type_draft/_sms,
    glue-vi-calculate), transaction_type_id (string 'PUR'/'CRD'/'CSH'/'UNK').

    IMPORTANTE: no asumir el mapeo por el significado aparente del código (ej.
    "códigos de cash -> CSH") -- en la tabla real, 19/25 son CRD (no CSH) y
    255 es UNK (no un sentinel genérico de "sin match", aunque el efecto de
    exclusión es el mismo). Siempre usar el join, nunca un case hardcodeado.
    """
    path = f"s3://{BUCKET_REF}/visa_business_transaction_type/data.parquet"
    try:
        return spark.read.parquet(path).select(
            F.col("business_transaction_type_id").cast(IntegerType()).alias("_btt_id"),
            F.col("transaction_type_id"),
        )
    except Exception as e:
        log_error(f"[load_visa_business_transaction_type] No encontrada en {path}: {e} — business_transaction_type_id saldra null (excluido por el filtro PUR/CRD/CSH)")
        return spark.createDataFrame([], schema="_btt_id int, transaction_type_id string")


def load_mastercard_business_transaction_type() -> DataFrame:
    """
    NUEVA tabla de referencia -- equivalente legacy operational.m_mastercard_business_transaction_type.
    Legacy: left join ... on business_transaction_type_id = left(processing_code,2)
    (getquery.py get_transactions_mastercard/get_on_us_mastercard, lineas 879/999).
    Sin filtro WHERE asociado (a diferencia de Visa) -- confirmado en ambas
    queries legacy, solo filtran por app_message_type in ('1240','1442').

    cols esperadas: business_transaction_type_id (string, 2 chars),
    transaction_type_id (string 'PUR'/'CRD'/'CSH'/'UNK').
    """
    path = f"s3://{BUCKET_REF}/mastercard_business_transaction_type/data.parquet"
    try:
        return spark.read.parquet(path).select(
            F.col("business_transaction_type_id").alias("_btt_id"),
            F.col("transaction_type_id"),
        )
    except Exception as e:
        log_error(f"[load_mastercard_business_transaction_type] No encontrada en {path}: {e} — business_transaction_type_id saldra null")
        return spark.createDataFrame([], schema="_btt_id string, transaction_type_id string")


def load_size_ticket() -> DataFrame:
    """
    NUEVA tabla de referencia (ver docstring del módulo). Columna real del id
    de bucket es `app_id` (no `size_ticket_id`), acá se renombra a
    size_ticket_id para el resto del script. brand real: 'MasterCard'/'VISA'
    (coincide con transaction_brand, a diferencia de scheme_fee_bin_products
    que usa 'MC'/'VISA').
    cols esperadas en el archivo: brand, country_code_list, ticket_currency,
    size_ticket_min, size_ticket_max, app_id
    """
    path = f"s3://{BUCKET_REF}/size_ticket/data.parquet"
    try:
        return spark.read.parquet(path).select(
            "brand", "country_code_list", "ticket_currency",
            "size_ticket_min", "size_ticket_max",
            F.col("app_id").alias("size_ticket_id"),
        )
    except Exception as e:
        log_error(f"[load_size_ticket] No encontrada en {path}: {e} — size_ticket saldrá siempre en 0")
        return spark.createDataFrame(
            [], schema="brand string, country_code_list string, ticket_currency string, "
                        "size_ticket_min double, size_ticket_max double, size_ticket_id int"
        )


def load_exchange_rates(start_date: str, end_date: str) -> DataFrame:
    """Copia de load_exchange_rates() en get_transaction.py, sin filtrar por brand
    (acá se filtra por brand en el momento de usarla, según haga falta).

    Fuente: exchange-rates-glue (enriquecido con codigos numericos por
    glue-exchange-rates, cobertura viva y actualizada) — reemplaza a
    exchange_rate/, fuente manual congelada al 2026-04-30. Particionada por
    brand + exchange_date; se lee al nivel base para que Spark descubra
    ambas columnas de particion (brand='Visa'/'Mastercard')."""
    path = f"s3://{BUCKET_REF}/exchange-rates-glue/"
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        log_error(f"[load_exchange_rates] No exchange rates at {path}: {e}")
        return spark.createDataFrame(
            [], schema="exchange_date string, brand string, from_currency string, to_currency string, fx_rate double"
        )
    df = df.select(
        F.col("exchange_date"),
        F.col("brand"),
        F.col("from_currency"),
        F.col("to_currency"),
        F.col("fx_rate"),
    )
    return df.filter(F.col("exchange_date").between(start_date, end_date))


def _join_fx(
    df: DataFrame, xrate_df: DataFrame, brand: str, date_col: str, from_col: str,
    alias: str, to_literal: str | None = None, to_col: str | None = None,
) -> DataFrame:
    """
    Join genérico de tipo de cambio: agrega `alias` con el fx_rate de
    from_col -> destino, en la fecha date_col, para la marca `brand`. Left
    join, null si no hay match (el llamador decide el fallback, usualmente
    coalesce a 1.0). El destino puede ser:
      - to_literal: un string fijo (ej. report_currency del cliente) — se
        filtra xrate_df antes del join, no requiere columna en df.
      - to_col: el nombre de una columna de df cuyo valor varía por fila
        (ej. ticket_currency, que depende de bank_country+brand).
    Exactamente uno de los dos debe pasarse.
    """
    assert (to_literal is None) != (to_col is None), "pasar exactamente uno de to_literal/to_col"

    xr = xrate_df.filter(F.upper(F.col("brand")) == brand.upper())
    if to_literal is not None:
        xr = xr.filter(F.col("to_currency") == to_literal)
    xr = xr.select(
        F.col("exchange_date").alias("_fx_date"),
        F.col("from_currency").alias("_fx_from"),
        F.col("to_currency").alias("_fx_to"),
        F.col("fx_rate").alias(alias),
    )
    cond = (df[date_col] == xr["_fx_date"]) & (df[from_col] == xr["_fx_from"])
    if to_col is not None:
        cond = cond & (df[to_col] == xr["_fx_to"])
    df = df.join(xr, cond, how="left").drop("_fx_date", "_fx_from", "_fx_to")
    return df


def _apply_duplicate_on_us(df: DataFrame) -> DataFrame:
    """
    Duplica filas on-us con business_mode_id='ACQUIRING' agregando una copia
    con business_mode_id='ISSUING'. Equivalente al bloque
    duplicate_on_us_flag_{visa,mastercard} del legacy (visa_on_us_query /
    mastercard_on_us_query) — mismo patrón que _apply_duplicate_on_us() en
    get_transaction.py.
    """
    dup_df = df.filter(
        (F.col("jurisdiction") == "on-us") & (F.col("business_mode_id") == "ACQUIRING")
    ).withColumn("business_mode_id", F.lit("ISSUING"))
    return df.union(dup_df)


def compute_visa_terminal_counts(vi_raw_df: DataFrame) -> DataFrame:
    """
    Equivalente a get_visa_monthly_terminal_count(): cuenta terminales
    distintos por card_acceptor_id, sobre TODO el mes y ambos file_type
    (IN + OUT), usada sólo para el umbral de greece_micropayment_indicator.
    """
    return (
        vi_raw_df.groupBy("card_acceptor_id")
        .agg(F.countDistinct("terminal_id").alias("count_terminal"))
    )


# =============================================================================
# TRANSFORM — VISA BASE II
# =============================================================================


def transform_visa_baseii_scheme_fee(
    df: DataFrame,
    client_cfg: dict,
    xrate_df: DataFrame,
    terminal_counts_df: DataFrame,
    scheme_fee_bin_products_df: DataFrame,
    visa_btt_df: DataFrame,
    local_switch_df: DataFrame,
    local_hashes: set,
) -> DataFrame:
    """
    Equivalente a get_issuers_visa() + get_acquirer_visa() del legacy (nuestro
    operational ya trae ISS+ACQ unificados en un solo Parquet, distinguidos
    por business_mode — no hace falta leerlos por separado).

    Notas de fidelidad al legacy:
    - business_transaction_type_id sale de un JOIN contra
      m_visa_business_transaction_type (visa_btt_df) usando el entero
      business_transaction_type calculado en glue-vi-calculate — no de un
      case sobre draft_code, que perdería la nuance de
      MCC/usage_code/special_condition_indicator que esa tabla codifica. El
      filtro posterior exige además que draft_code esté en
      (05,06,07,25,26,27), excluyendo códigos de reversal
      (15/16/17/35/36/37) aunque compartan la misma clasificación.
    - settlement_amount no existe como columna real en el operational BASEII
      (solo existe para VSS) — se deja NULL explícito, campo no usado aguas
      abajo.
    - business_mode_id se normaliza a mayúscula porque calc_business_mode usa
      minúscula en Mastercard y mayúscula en Visa (inconsistencia real entre
      los dos calculate.py).
    """
    report_currency = client_cfg["report_currency"]
    local_currency_code = client_cfg["local_currency_code"]
    customer_country = client_cfg["customer_country"]

    df = df.withColumn(
        "account_funding_source",
        F.when(F.col("funding_source").isin("C", "H", "R"), F.lit("C"))
        .when(F.col("funding_source").isin("D", "P"), F.lit("D"))
        .otherwise(F.lit(None).cast(StringType())),
    )

    df = df.join(
        visa_btt_df, df["business_transaction_type"] == visa_btt_df["_btt_id"], how="left"
    ).drop("_btt_id").withColumnRenamed("transaction_type_id", "business_transaction_type_id")

    df = df.filter(
        F.col("business_transaction_type_id").isin("PUR", "CRD", "CSH")
        & F.col("draft_code").isin("05", "06", "07", "25", "26", "27")
    )

    df = _resolve_switch_code(df, local_switch_df, local_hashes, CLIENT_CODE, "VISA")

    df = df.withColumn(
        "motoec_indicator",
        F.when(
            F.when(F.trim(F.col("moto_ec_indicator")) == "", F.lit(10))
            .otherwise(F.col("moto_ec_indicator").cast(IntegerType())) < 10,
            F.lit(0),
        ).otherwise(F.lit(1)),
    )

    df = df.withColumn(
        "currency_local_indicator",
        F.when(
            (F.col("source_currency_code_alphabetic") == F.lit(local_currency_code))
            | ((F.col("dcc_indicator") != "1") | F.col("dcc_indicator").isNull()),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    df = df.withColumn(
        "travel_program_indicator",
        F.when(F.col("product_id").isin("X", "X1"), F.lit(1)).otherwise(F.lit(0)),
    )

    df = df.join(
        terminal_counts_df,
        df["card_acceptor_id"] == terminal_counts_df["card_acceptor_id"],
        how="left",
    ).drop(terminal_counts_df["card_acceptor_id"])

    df = _join_fx(df, xrate_df, "VISA", "date", "source_currency_code_alphabetic", "exchange_rate", to_literal=report_currency)
    df = df.withColumn("exchange_rate", F.coalesce(F.col("exchange_rate"), F.lit(1.0)))
    df = df.withColumn("report_amount", F.col("exchange_rate") * F.col("source_amount"))

    is_greece_client = customer_country == "GRC"
    if is_greece_client:
        greece_cond = (
            F.col("merchant_category_code").isin(*GREECE_MICROPAYMENT_MCCS)
            & (F.col("jurisdiction") == "off-us")
            & F.col("product_id").isin(*GREECE_MICROPAYMENT_PRODUCT_IDS)
            & (
                (
                    F.col("moto_ec_indicator").isin("1", "2", "3", "4", "5", "6", "7", "8", "9")
                    & (F.col("report_amount") <= 10)
                )
                | (
                    ~F.col("moto_ec_indicator").isin("1", "2", "3", "4", "5", "6", "7", "8", "9")
                    & (F.col("report_amount") <= 20)
                )
            )
            & (F.coalesce(F.col("count_terminal"), F.lit(0)) <= 3)
        )
        df = df.withColumn("greece_micropayment_indicator", F.when(greece_cond, F.lit(1)).otherwise(F.lit(0)))
    else:
        df = df.withColumn("greece_micropayment_indicator", F.lit(0))

    product_ref = _product_ref_for_brand(scheme_fee_bin_products_df, "VISA")
    df = df.join(product_ref, df["product_id"] == product_ref["_pc"], how="left").drop("_pc")

    df = df.select(
        F.col("file_type").alias("app_type_file"),
        F.col("customer_code").alias("app_customer_code"),
        F.col("content_hash").alias("app_hash_file"),
        F.col("record").cast(LongType()).alias("app_id"),
        F.lit("VISA").alias("table_description"),
        F.col("date").alias("app_processing_date"),
        F.col("account_number").cast(StringType()).alias("account_number"),
        F.col("card_acceptor_id").cast(StringType()),
        F.col("account_funding_source"),
        F.col("ardef_country").alias("account_range_country"),
        F.col("product_id"),
        F.col("range_program_id").cast(IntegerType()),
        F.col("legacy_product_id").cast(IntegerType()),
        F.col("jurisdiction"),
        F.col("business_transaction_type_id"),
        F.col("reversal_indicator").cast(IntegerType()),
        F.col("currency_local_indicator").cast(IntegerType()),
        F.col("motoec_indicator").cast(IntegerType()),
        F.lit(None).cast(DoubleType()).alias("settlement_amount"),
        F.lit("VISA").alias("transaction_brand"),
        F.col("merchant_country_code"),
        F.col("switch_code").cast(IntegerType()),
        F.col("travel_program_indicator").cast(IntegerType()),
        F.col("greece_micropayment_indicator").cast(IntegerType()),
        F.lit(0).alias("key_entered_tpe"),
        F.col("settlement_report_currency_code").alias("settlement_currency"),
        F.lit(customer_country).alias("bank_country"),
        F.upper(F.col("business_mode")).alias("business_mode_id"),
        F.lit(0).cast(IntegerType()).alias("size_ticket"),
        F.col("purchase_date").cast(DateType()).alias("transaction_purchase_date"),
        F.col("exchange_rate").cast(DoubleType()),
        F.col("report_amount").cast(DoubleType()),
        F.lit(report_currency).alias("report_currency"),
    )
    return _apply_duplicate_on_us(df) if client_cfg["dup_on_us_visa"] else df


# =============================================================================
# TRANSFORM — VISA SMS (deshabilitado por defecto, ver ENABLE_SMS)
# =============================================================================


def transform_visa_sms_scheme_fee(
    df: DataFrame, client_cfg: dict, xrate_df: DataFrame, scheme_fee_bin_products_df: DataFrame,
    visa_btt_df: DataFrame, local_switch_df: DataFrame, local_hashes: set, currency_df: DataFrame,
) -> DataFrame:
    """
    Equivalente a get_sms_visa() del legacy. Estado pendiente de validación
    (mismo que transform_visa_sms en get_transaction.py) — nombres de columna
    raw (product_id_sms, pan_extended_country_code,
    mail_telephone_or_electronic_commerce_indicator, account_funding_source,
    dcc_indicator_sms, card_acceptor_terminal_id) tomados de
    dynamodb/items/visa_fields.json, no validados end-to-end contra un
    Parquet SMS real hasta la activación de ENABLE_SMS (2026-07-07).

    settlement_amount: a diferencia de BASEII (donde genuinamente no existe),
    SÍ existe como columna real (double) en el operational SMS — confirmado
    con Athena. La asunción original de "mismo motivo que BASEII" era
    incorrecta (nunca se verificó independientemente para SMS); corregido
    para usar la columna real.

    travel_program_indicator: usa `product_id` (calculado vía ARDEF,
    calc_product_id_ardef en glue-vi-calculate) — NO `product_id_sms` (crudo,
    que sí se usa para el join de range_program_id y la columna exportada
    "product_id"). Legacy distingue exactamente así (cf.product_id vs
    sms.product_id_sms) — nuestro código usaba product_id_sms para las 3
    cosas hasta este fix.

    settlement_currency: `settlement_currency_code_sms` es numérico
    (confirmado "710" para SBSA, no alfabético) — se convierte vía join
    contra currency_df, igual que legacy (join contra m_currency).

    currency_local_indicator: no existe una columna "transaction_currency_code"
    alfabética en el SMS real (confirmado con AnalysisException al activar
    ENABLE_SMS por primera vez) — el campo real es `draft_currency_code`
    (numérico), igual que usa `calc_source_currency_code_alphabetic_sms` ya
    validado en glue-vi-calculate. Se resuelve acá con el mismo join contra
    currency_df en vez de asumir una columna alfabética directa.

    report_amount/exchange_rate: legacy tiene una doble condición según
    `sms.transaction_amount = 0` (get_sms_visa, getquery.py ~651-652) — si es
    0, usa `cryptogram_currency_code`/(`cryptogram_amount` +
    `surcharge_amount_sms`) en vez de `transaction_currency_code`
    (`draft_currency_code` acá)/`transaction_amount` (`source_amount` acá).
    Confirmado con Athena (SBSA): 300,886 de 1,472,615 transacciones SMS
    (20.4%) tienen `source_amount=0` — sin este fix quedaban con
    `report_amount=0` en vez del monto real. Implementado con
    `_effective_currency_alpha`/`_effective_amount` antes del único
    `_join_fx()`.

    business_transaction_type_id usa el mismo JOIN y la misma tabla que
    transform_visa_baseii_scheme_fee (m_visa_business_transaction_type,
    llave = business_transaction_type calculado por
    calc_business_transaction_type_sms) — no se deriva de transaction_code_sms
    aunque ese campo también salga de business_transaction_type, para ser
    consistente con la fuente que usa BASEII. El filtro exige además
    transaction_code_sms en (05,06,07,25,26,27), igual que legacy.
    """
    report_currency = client_cfg["report_currency"]
    local_currency_code = client_cfg["local_currency_code"]
    customer_country = client_cfg["customer_country"]

    df = df.filter(F.col("local_draft_date").isNotNull())

    df = df.join(
        visa_btt_df, df["business_transaction_type"] == visa_btt_df["_btt_id"], how="left"
    ).drop("_btt_id").withColumnRenamed("transaction_type_id", "business_transaction_type_id")

    df = df.filter(
        F.col("business_transaction_type_id").isin("PUR", "CRD", "CSH")
        & F.col("transaction_code_sms").isin("05", "06", "07", "25", "26", "27")
    )

    df = _resolve_switch_code(df, local_switch_df, local_hashes, CLIENT_CODE, "VISA")

    df = df.withColumn(
        "motoec_indicator",
        F.when(
            F.when(F.trim(F.col("mail_telephone_or_electronic_commerce_indicator")) == "", F.lit(10))
            .otherwise(F.col("mail_telephone_or_electronic_commerce_indicator").cast(IntegerType())) < 10,
            F.lit(0),
        ).otherwise(F.lit(1)),
    )
    currency_alpha_ref = currency_df.select(
        F.col("currency_numeric_code").alias("_currency_numeric"),
        F.col("currency_alphabetic_code").alias("_draft_currency_alpha"),
    )
    df = df.join(
        currency_alpha_ref, df["draft_currency_code"] == currency_alpha_ref["_currency_numeric"], how="left"
    ).drop("_currency_numeric")

    df = df.withColumn(
        "currency_local_indicator",
        F.when(
            (F.col("_draft_currency_alpha") == F.lit(local_currency_code))
            | ((F.col("dcc_indicator_sms") != "1") | F.col("dcc_indicator_sms").isNull()),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    df = df.withColumn(
        "account_funding_source",
        F.when(F.col("account_funding_source").isin("C", "H", "R"), F.lit("C"))
        .when(F.col("account_funding_source").isin("D", "P"), F.lit("D"))
        .otherwise(F.lit(None).cast(StringType())),
    )
    # Legacy usa cf.product_id (calculado via ARDEF, calc_product_id_ardef en
    # glue-vi-calculate) para este indicador especificamente -- distinto de
    # sms.product_id_sms (crudo), que sigue usandose para el join de
    # range_program_id y la columna exportada "product_id".
    df = df.withColumn(
        "travel_program_indicator",
        F.when(F.col("product_id").isin("X", "X1"), F.lit(1)).otherwise(F.lit(0)),
    )

    # Legacy (get_sms_visa, getquery.py ~651-652): doble condicion cuando
    # transaction_amount=0 -- usa cryptogram_currency_code/(cryptogram_amount
    # + surcharge_amount_sms) en vez de transaction_currency_code/
    # transaction_amount. Sin esto, el 20.4% de las transacciones SMS de SBSA
    # (source_amount=0) quedaban con report_amount=0 en vez del monto real.
    crypto_currency_ref = currency_df.select(
        F.col("currency_numeric_code").alias("_currency_numeric_crypto"),
        F.col("currency_alphabetic_code").alias("_cryptogram_currency_alpha"),
    )
    df = df.join(
        crypto_currency_ref, df["cryptogram_currency_code"] == crypto_currency_ref["_currency_numeric_crypto"], how="left"
    ).drop("_currency_numeric_crypto")

    # settlement_currency_code_sms es numerico (confirmado "710" para SBSA,
    # no alfabetico) -- legacy lo convierte via join contra m_currency
    # (c.currency_alphabetic_code); acá igual, para la columna "settlement_currency".
    settlement_currency_ref = currency_df.select(
        F.col("currency_numeric_code").alias("_currency_numeric_settlement"),
        F.col("currency_alphabetic_code").alias("_settlement_currency_alpha"),
    )
    df = df.join(
        settlement_currency_ref,
        df["settlement_currency_code_sms"] == settlement_currency_ref["_currency_numeric_settlement"],
        how="left",
    ).drop("_currency_numeric_settlement")

    df = df.withColumn(
        "_effective_currency_alpha",
        F.when(F.col("source_amount") == 0, F.col("_cryptogram_currency_alpha")).otherwise(F.col("_draft_currency_alpha")),
    ).withColumn(
        "_effective_amount",
        F.when(
            F.col("source_amount") == 0,
            F.coalesce(F.col("cryptogram_amount"), F.lit(0.0)) + F.coalesce(F.col("surcharge_amount_sms"), F.lit(0.0)),
        ).otherwise(F.col("source_amount")),
    ).drop("_draft_currency_alpha", "_cryptogram_currency_alpha")

    df = _join_fx(df, xrate_df, "VISA", "date", "_effective_currency_alpha", "exchange_rate", to_literal=report_currency)
    df = df.withColumn("exchange_rate", F.coalesce(F.col("exchange_rate"), F.lit(1.0)))
    df = df.withColumn("report_amount", F.col("exchange_rate") * F.col("_effective_amount")).drop(
        "_effective_currency_alpha", "_effective_amount"
    )

    product_ref = _product_ref_for_brand(scheme_fee_bin_products_df, "VISA")
    df = df.join(product_ref, df["product_id_sms"] == product_ref["_pc"], how="left").drop("_pc")

    df = df.select(
        F.col("file_type").alias("app_type_file"),
        F.col("customer_code").alias("app_customer_code"),
        F.col("content_hash").alias("app_hash_file"),
        F.col("record").cast(LongType()).alias("app_id"),
        F.lit("VISA SMS").alias("table_description"),
        F.col("date").alias("app_processing_date"),
        F.col("card_number").cast(StringType()).alias("account_number"),
        F.col("card_acceptor_terminal_id").cast(StringType()).alias("card_acceptor_id"),
        F.col("account_funding_source"),
        F.col("pan_extended_country_code").cast(StringType()).alias("account_range_country"),
        F.col("product_id_sms").alias("product_id"),
        F.col("range_program_id").cast(IntegerType()),
        F.col("legacy_product_id").cast(IntegerType()),
        F.col("jurisdiction"),
        F.col("business_transaction_type_id"),
        F.col("reversal_indicator").cast(IntegerType()),
        F.col("currency_local_indicator").cast(IntegerType()),
        F.col("motoec_indicator").cast(IntegerType()),
        F.col("settlement_amount").cast(DoubleType()),
        F.lit("VISA").alias("transaction_brand"),
        F.col("card_acceptor_country").alias("merchant_country_code"),
        F.col("switch_code").cast(IntegerType()),
        F.col("travel_program_indicator").cast(IntegerType()),
        F.lit(0).alias("greece_micropayment_indicator"),
        F.lit(0).alias("key_entered_tpe"),
        F.col("_settlement_currency_alpha").alias("settlement_currency"),
        F.lit(customer_country).alias("bank_country"),
        F.upper(F.col("business_mode")).alias("business_mode_id"),
        F.lit(0).cast(IntegerType()).alias("size_ticket"),
        F.col("local_draft_date").cast(DateType()).alias("transaction_purchase_date"),
        F.col("exchange_rate").cast(DoubleType()),
        F.col("report_amount").cast(DoubleType()),
        F.lit(report_currency).alias("report_currency"),
    )
    return _apply_duplicate_on_us(df) if client_cfg["dup_on_us_visa"] else df


# =============================================================================
# TRANSFORM — MASTERCARD (MTI 1240 y 1442)
# =============================================================================


def transform_mastercard_scheme_fee(
    df: DataFrame,
    client_cfg: dict,
    currency_df: DataFrame,
    xrate_df: DataFrame,
    scheme_fee_bin_products_df: DataFrame,
    mc_btt_df: DataFrame,
    local_switch_df: DataFrame,
    local_hashes: set,
) -> DataFrame:
    """
    Equivalente a get_transactions_mastercard() del legacy. Reutiliza el mismo
    patrón de swap IN/OUT + MTI 1240/1442 ya validado en
    transform_mastercard() de get_transaction.py (merchant/issuer country,
    monto/moneda de transacción, swap account_range_country/merchant_country_code).

    Puntos donde se aparta deliberadamente de get_transaction.py, por
    fidelidad al legacy de scheme fee:
    - _effective_product_id usa gcms_product_identifier directo (legacy:
      getquery.py ~877), sin el coalesce(licensed_product_identifier_pds_3,
      gcms_product_identifier) que sí usa get_transaction.py para
      product_program_id — acá cambiaría legacy_product_id/range_program_id.
    - account_funding_source usa funding_source crudo, sin el colapso
      C/H/R->C y D/P->D que sí aplican transform_visa_baseii_scheme_fee/
      transform_visa_sms_scheme_fee (legacy: getquery.py ~826/936, Mastercard
      nunca colapsa este campo).
    - reversal_indicator reutiliza la expresión ya validada de
      get_transaction.py en vez de la fórmula original del legacy de scheme
      fee (ver docstring del módulo).

    business_transaction_type_id sale de un JOIN contra
    m_mastercard_business_transaction_type (mc_btt_df), llave =
    left(processing_code, 2) — igual que Visa, no un case hardcodeado (legacy:
    getquery.py ~879). A diferencia de Visa, no hay filtro WHERE asociado.

    La moneda de la transacción (_src_currency_numeric) hace el mismo swap
    IN/OUT + lpad(3,'0') que get_transaction.py, aunque el legacy original de
    scheme fee no lo hacía (usaba currency_code_transaction siempre) — sin el
    swap, el fee y currency_local_indicator de los archivos IN saldrían con
    la moneda equivocada sobre este operational.
    """
    report_currency = client_cfg["report_currency"]
    customer_country = client_cfg["customer_country"]
    local_currency_code = client_cfg["local_currency_code"]
    is_1442 = F.col("type_mti") == "1442"

    df = df.withColumn(
        "_effective_product_id",
        F.col("gcms_product_identifier"),
    )

    df = df.withColumn(
        "account_funding_source",
        F.col("funding_source"),
    )

    df = df.withColumn(
        "account_range_country",
        F.when(~is_1442, F.col("iar_country")).when(is_1442, F.col("card_acceptor_country_code_de_43_6")),
    ).withColumn(
        "merchant_country_code",
        F.when(~is_1442, F.col("card_acceptor_country_code_de_43_6")).when(is_1442, F.col("iar_country")),
    )

    df = df.withColumn("_btt_key", F.col("processing_code_de_3").substr(1, 2))
    df = df.join(
        mc_btt_df, df["_btt_key"] == mc_btt_df["_btt_id"], how="left"
    ).drop("_btt_key", "_btt_id").withColumnRenamed("transaction_type_id", "business_transaction_type_id")

    df = _resolve_switch_code(df, local_switch_df, local_hashes, CLIENT_CODE, "MasterCard")

    df = df.withColumn(
        "reversal_indicator",
        F.when(
            (~is_1442)
            & F.col("message_reversal_indicator_pds_25").isNotNull()
            & (F.col("message_reversal_indicator_pds_25") != ""),
            F.lit(1),
        )
        .when(
            is_1442
            & (F.col("message_reversal_indicator_pds_25").isNull() | (F.col("message_reversal_indicator_pds_25") == "")),
            F.lit(1),
        )
        .otherwise(F.lit(0)),
    )

    df = df.withColumn(
        "_src_currency_numeric",
        F.lpad(
            F.when(F.col("file_type") == "IN", F.col("currency_code_reconciliation_de_50"))
            .otherwise(F.col("currency_code_transaction_de_49"))
            .cast(StringType()),
            3, "0",
        ),
    )
    currency_alpha_ref = currency_df.select(
        F.col("currency_numeric_code").alias("_currency_numeric"),
        F.col("currency_alphabetic_code").alias("cur_alpha"),
    )
    df = df.join(
        currency_alpha_ref,
        df["_src_currency_numeric"] == currency_alpha_ref["_currency_numeric"],
        how="left",
    ).drop("_currency_numeric", "_src_currency_numeric")

    df = df.withColumn(
        "currency_local_indicator",
        F.when(F.col("cur_alpha") == F.lit(local_currency_code), F.lit(1)).otherwise(F.lit(0)),
    )
    df = df.withColumn(
        "motoec_indicator",
        F.when(F.col("pos_entry_mode_de_22").substr(6, 1) == "0", F.lit(0)).otherwise(F.lit(1)),
    )

    df = df.withColumn(
        "travel_program_indicator",
        F.when(
            (F.col("business_activity_4_pds_158_4") == "BB")
            & (
                F.col("gcms_product_identifier").isin(*MC_TRAVEL_PRODUCT_IDS_A)
                | (
                    F.col("gcms_product_identifier").isin(*MC_TRAVEL_PRODUCT_IDS_B)
                    & (
                        F.col("card_acceptor_business_code_[mcc]_de_26").between(*MC_TRAVEL_MCC_RANGE)
                        | F.col("card_acceptor_business_code_[mcc]_de_26").isin(*MC_TRAVEL_MCC_LIST)
                    )
                )
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    df = df.withColumn(
        "key_entered_tpe",
        F.when(
            (F.col("pos_entry_mode_de_22").substr(7, 1) == "6")
            & (F.col("jurisdiction") == "intraregional")
            & F.col("account_range_country").isin(*GREECE_EU_COUNTRIES),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    df = df.withColumn(
        "_amount_num",
        F.when(F.col("file_type") == "IN", F.col("amount_reconciliation_de_5"))
        .otherwise(F.col("amount_transaction_de_4")),
    )
    df = _join_fx(df, xrate_df, "MasterCard", "date", "cur_alpha", "exchange_rate", to_literal=report_currency)
    df = df.withColumn("exchange_rate", F.coalesce(F.col("exchange_rate"), F.lit(1.0)))
    df = df.withColumn("report_amount", F.col("exchange_rate") * F.col("_amount_num"))

    product_ref = _product_ref_for_brand(scheme_fee_bin_products_df, "MC")
    df = df.join(product_ref, df["_effective_product_id"] == product_ref["_pc"], how="left").drop("_pc")

    df = df.select(
        F.col("file_type").alias("app_type_file"),
        F.col("customer_code").alias("app_customer_code"),
        F.col("content_hash").alias("app_hash_file"),
        F.monotonically_increasing_id().cast(LongType()).alias("app_id"),
        F.lit("MasterCard").alias("table_description"),
        F.col("date").alias("app_processing_date"),
        F.col("pan_de_2").cast(StringType()).substr(1, 16).alias("account_number"),
        F.col("card_acceptor_id_code_de_42").cast(StringType()).alias("card_acceptor_id"),
        F.col("account_funding_source"),
        F.col("account_range_country"),
        F.col("_effective_product_id").alias("product_id"),
        F.col("range_program_id").cast(IntegerType()),
        F.col("legacy_product_id").cast(IntegerType()),
        F.col("jurisdiction"),
        F.col("business_transaction_type_id"),
        F.col("reversal_indicator").cast(IntegerType()),
        F.col("currency_local_indicator").cast(IntegerType()),
        F.col("motoec_indicator").cast(IntegerType()),
        F.col("settlement_report_amount").cast(DoubleType()).alias("settlement_amount"),
        F.lit("MasterCard").alias("transaction_brand"),
        F.col("merchant_country_code"),
        F.col("switch_code").cast(IntegerType()),
        F.col("travel_program_indicator").cast(IntegerType()),
        F.lit(0).alias("greece_micropayment_indicator"),
        F.col("key_entered_tpe").cast(IntegerType()),
        F.col("settlement_report_currency_code").alias("settlement_currency"),
        F.lit(customer_country).alias("bank_country"),
        F.upper(F.col("business_mode")).alias("business_mode_id"),
        F.lit(0).cast(IntegerType()).alias("size_ticket"),
        F.col("date_and_time_local_transaction_de_12").cast(DateType()).alias("transaction_purchase_date"),
        F.col("exchange_rate").cast(DoubleType()),
        F.col("report_amount").cast(DoubleType()),
        F.lit(report_currency).alias("report_currency"),
    )
    return _apply_duplicate_on_us(df) if client_cfg["dup_on_us_mc"] else df


# =============================================================================
# SIZE TICKET (resuelto post-union, igual que update_size_tickets() del legacy)
# =============================================================================


def resolve_size_ticket(df: DataFrame, size_ticket_df: DataFrame, xrate_df: DataFrame) -> DataFrame:
    """
    Convierte report_amount a la moneda de "ticket" del país+marca del
    cliente (m_size_ticket.ticket_currency) y matchea el bucket
    [size_ticket_min, size_ticket_max) para asignar size_ticket. Default 0 si
    no hay match (igual que legacy: COALESCE(x.size_ticket, 0)).
    """
    pre = size_ticket_df.select(
        F.col("country_code_list"),
        F.col("brand").alias("transaction_brand_ref"),
        F.col("ticket_currency"),
    ).distinct()

    df = df.join(
        pre,
        (df["transaction_brand"] == pre["transaction_brand_ref"]) & (df["bank_country"] == pre["country_code_list"]),
        how="left",
    ).drop("country_code_list")

    df = _join_fx(df, xrate_df, "VISA", "app_processing_date", "report_currency", "_fx_to_ticket_visa", to_col="ticket_currency")
    df = _join_fx(df, xrate_df, "MasterCard", "app_processing_date", "report_currency", "_fx_to_ticket_mc", to_col="ticket_currency")
    df = df.withColumn(
        "_fx_to_ticket",
        F.when(F.col("transaction_brand") == "VISA", F.col("_fx_to_ticket_visa")).otherwise(F.col("_fx_to_ticket_mc")),
    ).drop("_fx_to_ticket_visa", "_fx_to_ticket_mc")

    df = df.withColumn(
        "_ticket_amount",
        F.when(F.col("report_currency") == F.col("ticket_currency"), F.col("report_amount"))
        .otherwise(F.col("report_amount") * F.col("_fx_to_ticket")),
    )

    buckets = size_ticket_df.select(
        F.col("brand").alias("_st_brand"),
        F.col("country_code_list").alias("_st_country"),
        F.col("size_ticket_min").alias("_st_min"),
        F.col("size_ticket_max").alias("_st_max"),
        F.col("size_ticket_id").alias("_st_id"),
    )
    df = df.join(
        buckets,
        (df["transaction_brand"] == buckets["_st_brand"])
        & (df["bank_country"] == buckets["_st_country"])
        & (df["_ticket_amount"] >= buckets["_st_min"])
        & (df["_ticket_amount"] < buckets["_st_max"]),
        how="left",
    )
    df = df.withColumn("size_ticket", F.coalesce(F.col("_st_id"), F.lit(0)).cast(IntegerType()))
    return df.drop("ticket_currency", "_fx_to_ticket", "_ticket_amount", "_st_brand", "_st_country", "_st_min", "_st_max", "_st_id", "transaction_brand_ref")


# =============================================================================
# AGREGACION A NIVEL REPORTE
# =============================================================================


def aggregate_to_report(detail_df: DataFrame) -> DataFrame:
    """Equivalente a get_insert_into_report_table(): agrupa por las 20
    dimensiones y suma transaction_count / report_amount."""
    return (
        detail_df.groupBy(*GROUP_DIMS)
        .agg(
            F.count(F.lit(1)).alias("transaction_count"),
            F.sum("report_amount").alias("transaction_amount"),
            F.first("legacy_product_id", ignorenulls=True).alias("legacy_product_id"),
        )
        .withColumn("transaction_scheme_fee_cost", F.lit(0.0))
        .withColumn("unitary_scheme_fee_cost", F.lit(0.0))
        .withColumn("estimated_scheme_fee_cost", F.lit(0.0))
        .withColumn("unitary_estimated_scheme_fee_cost", F.lit(0.0))
    )


# =============================================================================
# EXPORT CSV LEGACY (mode=generate)
# =============================================================================


def build_legacy_export(
    report_pdf: pd.DataFrame, country_pdf: pd.DataFrame, bin_funding_pdf: pd.DataFrame,
    execution_id: str, report_month: str, client_code: str,
) -> pd.DataFrame:
    """
    Arma el CSV con el layout legacy exacto (get_report_legacy_columns_filter),
    incluyendo los sentinels de nulos (255 para columnas *_id/*_ind, 0 para
    columnas de conteo/monto, '9999999999999999' para columnas de texto) —
    mismo comportamiento que managment.py:generate_table() al final, justo
    antes del to_csv().

    mct_cd hace fillna ANTES de astype(str): un account_number nulo se
    convertiría a la cadena literal "nan"/"None" con astype(str) directo, que
    el fillna genérico de más abajo no detecta (solo atrapa NaN real) — mismo
    patrón de bug ya documentado en glue-vi-interchange (_apply_default,
    gotchas.md).
    """
    df = report_pdf.copy()
    df["app_id"] = range(1, len(df) + 1)
    df["app_execution_id"] = execution_id
    df["rpt_bnk_id"] = client_code
    df["set_mth"] = int(report_month)
    df["bus_id"] = df["business_mode_id"].map({"ACQUIRING": 1, "ISSUING": 2})
    df["sch_id"] = df["transaction_brand"].map({"MasterCard": 1, "VISA": 2})
    df["tkt_siz_id"] = df["size_ticket"]
    df["prd_id"] = df["legacy_product_id"]
    df["prg_id"] = df["range_program_id"]

    fnd_map = dict(zip(bin_funding_pdf.get("bin_funding_source_code", []), bin_funding_pdf.get("bin_funding_source_mc_numeric_code", [])))
    df["fnd_src_id"] = df["account_funding_source"].map(fnd_map)

    mc_scope = {"on-us": 1, "off-us": 2, "intraregional": 4, "interregional": 8}
    vi_scope = {"on-us": 1, "off-us": 2, "intraregional": 3, "interregional": 4}
    df["txn_scp_id"] = df.apply(
        lambda r: (mc_scope if r["transaction_brand"] == "MasterCard" else vi_scope).get(r["jurisdiction"]),
        axis=1,
    )
    df["txn_typ_id"] = df["business_transaction_type_id"].map({"PUR": 1, "CSH": 8, "CRD": 1})
    df["txn_rvsl_flg_id"] = df["reversal_indicator"]
    df["txn_crncy_lcl_flg_id"] = df["currency_local_indicator"]
    df["txn_crd_prs_flg_id"] = df["motoec_indicator"]
    df["mct_cd"] = df["account_number"].fillna("9999999999999999").astype(str).str.slice(0, 16)
    df["txn_cnt"] = df["transaction_count"]
    df["txn_amt"] = df["transaction_amount"]
    df["txn_sfc"] = df["transaction_scheme_fee_cost"]

    country_vi = dict(zip(country_pdf["country_code"], country_pdf["legacy_country_id"]))
    country_mc = dict(zip(country_pdf["country_code_alternative"], country_pdf["legacy_country_id"]))
    df["mct_ctry_id"] = df.apply(
        lambda r: country_mc.get(r["merchant_country_code"]) if r["transaction_brand"] == "MasterCard"
        else country_vi.get(r["merchant_country_code"]),
        axis=1,
    )
    df["unt_sfc"] = df["unitary_scheme_fee_cost"]
    df["est_sch_fee_amt"] = df["estimated_scheme_fee_cost"]
    df["unt_est_sch_fee_amt"] = df["unitary_estimated_scheme_fee_cost"]
    df["swt_cd"] = df["switch_code"]
    df["trvl_prg_ind"] = df["travel_program_indicator"]
    df["grc_mpymt_ind"] = df["greece_micropayment_indicator"]
    df["key_etrd_tpe"] = df["key_entered_tpe"]

    export_cols = [
        "app_id", "app_execution_id", "rpt_bnk_id", "set_mth", "bus_id", "sch_id",
        "tkt_siz_id", "prd_id", "prg_id", "fnd_src_id", "txn_scp_id", "txn_typ_id",
        "txn_rvsl_flg_id", "txn_crncy_lcl_flg_id", "txn_crd_prs_flg_id", "mct_cd",
        "txn_cnt", "txn_amt", "txn_sfc", "mct_ctry_id", "unt_sfc", "est_sch_fee_amt",
        "unt_est_sch_fee_amt", "swt_cd", "trvl_prg_ind", "grc_mpymt_ind", "key_etrd_tpe",
    ]
    out = df[export_cols].copy()

    numeric_excluded = ["app_id", "set_mth", "txn_cnt", "txn_amt", "txn_sfc", "unt_sfc", "est_sch_fee_amt", "unt_est_sch_fee_amt"]
    numeric_cols = [c for c in out.select_dtypes(include=["number"]).columns if c not in numeric_excluded]
    out[numeric_cols] = out[numeric_cols].fillna(255)

    fill_zero_cols = ["txn_cnt", "txn_amt", "txn_sfc", "unt_sfc", "est_sch_fee_amt", "unt_est_sch_fee_amt"]
    out[fill_zero_cols] = out[fill_zero_cols].fillna(0)

    text_excluded = ["app_execution_id", "rpt_bnk_id"]
    text_cols = [c for c in out.select_dtypes(include=["object"]).columns if c not in text_excluded]
    out[text_cols] = out[text_cols].fillna("9999999999999999")

    cast_map = {
        "app_id": int, "set_mth": int, "bus_id": int, "sch_id": int, "tkt_siz_id": int,
        "prd_id": int, "prg_id": int, "fnd_src_id": int, "txn_scp_id": int, "txn_typ_id": int,
        "txn_rvsl_flg_id": int, "txn_crncy_lcl_flg_id": int, "txn_crd_prs_flg_id": int,
        "mct_cd": str, "swt_cd": int, "trvl_prg_ind": int, "grc_mpymt_ind": int,
        "key_etrd_tpe": int, "txn_cnt": int, "txn_amt": float, "txn_sfc": float,
        "mct_ctry_id": int, "unt_sfc": float, "est_sch_fee_amt": float, "unt_est_sch_fee_amt": float,
    }
    out = out.astype(cast_map, errors="ignore")
    return out


def write_csv_to_s3(pdf: pd.DataFrame, bucket: str, key: str):
    csv_body = pdf.to_csv(sep=",", index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_body.encode("utf-8"))
    log_info(f"[write_csv_to_s3] Escrito s3://{bucket}/{key} ({len(pdf)} filas)")


def _s3_key_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def _write_json_to_s3(obj: dict, bucket: str, key: str):
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(obj, default=str).encode("utf-8"))


def _read_json_from_s3(bucket: str, key: str) -> dict:
    resp = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read())


# =============================================================================
# MODE = GENERATE
# =============================================================================


def run_generate():
    """
    Orquesta el paso --mode generate (ver docstring del módulo). El app_id
    que build_legacy_export() asigna al CSV de export se copia también al
    report_pdf que se persiste como estado interno, porque --mode read
    necesita matchear por ese mismo app_id.
    """
    year = int(REPORT_MONTH[0:4])
    month = int(REPORT_MONTH[4:6])
    last_day = monthrange(year, month)[1]
    start_date = f"{year:04d}-{month:02d}-01"
    end_date = f"{year:04d}-{month:02d}-{last_day:02d}"

    summary_key = f"{STATE_PREFIX}/summary.json"
    if _s3_key_exists(ANALYTICS_BUCKET, summary_key) and not FORCE:
        raise RuntimeError(
            f"Ya existe un reporte generado para {CLIENT_CODE}/{REPORT_MONTH} "
            f"(s3://{ANALYTICS_BUCKET}/{summary_key}). Pasar --force true para sobreescribir."
        )

    client_cfg = get_client_config(CLIENT_CODE)
    log_info(f"[run_generate] client_cfg={client_cfg}")

    country_df = load_country()
    currency_df = load_currency()
    scheme_fee_bin_products_df = load_scheme_fee_bin_products()
    bin_funding_df = load_bin_funding_source()
    size_ticket_df = load_size_ticket()
    xrate_df = load_exchange_rates(start_date, end_date)
    visa_btt_df = load_visa_business_transaction_type()
    mc_btt_df = load_mastercard_business_transaction_type()
    local_switch_df = load_local_switch()
    local_hashes = get_local_switch_content_hashes(CLIENT_CODE)

    detail_frames = []

    vi_raw = read_operational(CLIENT_CODE, "VISA", "baseii_drafts", start_date, end_date)
    if vi_raw is not None:
        terminal_counts_df = compute_visa_terminal_counts(vi_raw)
        detail_frames.append(
            transform_visa_baseii_scheme_fee(vi_raw, client_cfg, xrate_df, terminal_counts_df, scheme_fee_bin_products_df, visa_btt_df, local_switch_df, local_hashes)
        )

    if ENABLE_SMS:
        sms_raw = read_operational(CLIENT_CODE, "VISA", "sms_messages", start_date, end_date)
        if sms_raw is not None:
            detail_frames.append(transform_visa_sms_scheme_fee(sms_raw, client_cfg, xrate_df, scheme_fee_bin_products_df, visa_btt_df, local_switch_df, local_hashes, currency_df))

    for mti in ("IPM_1240", "IPM_1442"):
        mc_raw = read_operational(CLIENT_CODE, "MC", mti, start_date, end_date)
        if mc_raw is not None:
            detail_frames.append(
                transform_mastercard_scheme_fee(mc_raw, client_cfg, currency_df, xrate_df, scheme_fee_bin_products_df, mc_btt_df, local_switch_df, local_hashes)
            )

    if not detail_frames:
        raise RuntimeError(f"No hay datos operational para {CLIENT_CODE} en [{start_date}, {end_date}]")

    detail_df = detail_frames[0]
    for f_df in detail_frames[1:]:
        detail_df = detail_df.unionByName(f_df)

    detail_df = resolve_size_ticket(detail_df, size_ticket_df, xrate_df)
    detail_df = detail_df.withColumn("scheme_fee_execution_month", F.lit(REPORT_MONTH)).cache()

    detail_count = detail_df.count()
    log_info(f"[run_generate] detail rows: {detail_count}")

    report_df = aggregate_to_report(detail_df)
    report_pdf = report_df.toPandas()
    log_info(f"[run_generate] report groups: {len(report_pdf)}")

    now = datetime.now()
    execution_id = f"{now.year}{now.month}{now.day}_{int(now.timestamp())}"

    country_pdf = country_df.toPandas()
    bin_funding_pdf = bin_funding_df.toPandas()
    export_pdf = build_legacy_export(report_pdf, country_pdf, bin_funding_pdf, execution_id, REPORT_MONTH, CLIENT_CODE)

    filename = f"{CLIENT_CODE}_{REPORT_MONTH}_{execution_id}.csv"
    out_key = f"OUT/{CLIENT_CODE}/{filename}"
    write_csv_to_s3(export_pdf, SCHEME_FEE_BUCKET, out_key)

    report_pdf["app_id"] = export_pdf["app_id"].values

    detail_df.write.mode("overwrite").parquet(f"s3://{ANALYTICS_BUCKET}/{STATE_PREFIX}/detail/")
    spark.createDataFrame(report_pdf).write.mode("overwrite").parquet(f"s3://{ANALYTICS_BUCKET}/{STATE_PREFIX}/report/")

    summary = {
        "client_code": CLIENT_CODE,
        "report_month": REPORT_MONTH,
        "execution_id": execution_id,
        "number_of_groups": len(report_pdf),
        "number_of_inserted_rows": detail_count,
        "s3_route_out": f"s3://{SCHEME_FEE_BUCKET}/{out_key}",
        "generated_at": now.isoformat(),
    }
    _write_json_to_s3(summary, ANALYTICS_BUCKET, summary_key)
    log_info(f"[run_generate] OK — {json.dumps(summary)}")


# =============================================================================
# MODE = READ
# =============================================================================


def update_report_and_propagate():
    """
    Nota importante sobre transaction_scheme_fee_cost vs unt_sfc: el legacy
    (get_update_detail) copia el TOTAL del grupo (txn_sfc) en
    transaction_scheme_fee_cost de CADA fila de detalle que matchea ese
    grupo — no lo divide por transaction_count. Esto se replica tal cual acá
    por fidelidad, pero para cualquier consumo futuro (ej. scheme_fees_amount
    en get_transaction.py) debe usarse la columna UNITARIA (unt_sfc), sumar
    transaction_scheme_fee_cost por fila sobrecontaría el costo total del
    grupo tantas veces como transacciones tenga.
    """
    summary_key = f"{STATE_PREFIX}/summary.json"
    if not _s3_key_exists(ANALYTICS_BUCKET, summary_key):
        raise RuntimeError(
            f"No existe un reporte generado (summary.json) para {CLIENT_CODE}/{REPORT_MONTH}. "
            "Correr --mode generate primero."
        )
    summary = _read_json_from_s3(ANALYTICS_BUCKET, summary_key)

    in_key = f"IN/{CLIENT_CODE}/{IN_FILE_KEY}"
    log_info(f"[run_read] Leyendo s3://{SCHEME_FEE_BUCKET}/{in_key}")
    resp = s3.get_object(Bucket=SCHEME_FEE_BUCKET, Key=in_key)
    valid_nans = ["-1.#IND", "1.#QNAN", "1.#IND", "-1.#QNAN", "#N/A N/A", "#N/A",
                  "N/A", "n/a", "", "#NA", "NULL", "null", "NaN", "-NaN", "nan", "-nan"]
    in_df = pd.read_csv(
        resp["Body"], sep=",", encoding="Latin-1", keep_default_na=False, na_values=valid_nans,
        dtype={"mct_cd": str, "prg_id": str},
    )

    if len(in_df) == 0:
        log_info("[run_read] Archivo sin datos — cerrando sin cambios")
        return

    if len(in_df) != summary["number_of_groups"]:
        raise RuntimeError(
            f"La cantidad de filas del archivo ({len(in_df)}) no coincide con la del "
            f"reporte generado ({summary['number_of_groups']}). Abortando (igual que legacy)."
        )

    in_df["unt_sfc"] = (in_df["txn_sfc"] / in_df["txn_cnt"]).round(6)
    in_df["unt_est_sch_fee_amt"] = (in_df["est_sch_fee_amt"] / in_df["txn_cnt"]).round(6)

    report_df = spark.read.parquet(f"s3://{ANALYTICS_BUCKET}/{STATE_PREFIX}/report/")
    updates_df = spark.createDataFrame(
        in_df[["app_id", "txn_sfc", "unt_sfc", "est_sch_fee_amt", "unt_est_sch_fee_amt"]]
    ).withColumnRenamed("txn_sfc", "_new_txn_sfc") \
     .withColumnRenamed("unt_sfc", "_new_unt_sfc") \
     .withColumnRenamed("est_sch_fee_amt", "_new_est_sch_fee_amt") \
     .withColumnRenamed("unt_est_sch_fee_amt", "_new_unt_est_sch_fee_amt")

    updated_report_df = (
        report_df.join(updates_df, on="app_id", how="left")
        .withColumn("transaction_scheme_fee_cost", F.coalesce(F.col("_new_txn_sfc"), F.col("transaction_scheme_fee_cost")))
        .withColumn("unitary_scheme_fee_cost", F.coalesce(F.col("_new_unt_sfc"), F.col("unitary_scheme_fee_cost")))
        .withColumn("estimated_scheme_fee_cost", F.coalesce(F.col("_new_est_sch_fee_amt"), F.col("estimated_scheme_fee_cost")))
        .withColumn("unitary_estimated_scheme_fee_cost", F.coalesce(F.col("_new_unt_est_sch_fee_amt"), F.col("unitary_estimated_scheme_fee_cost")))
        .drop("_new_txn_sfc", "_new_unt_sfc", "_new_est_sch_fee_amt", "_new_unt_est_sch_fee_amt")
    )
    updated_report_count = updated_report_df.count()
    if updated_report_count != summary["number_of_groups"]:
        raise RuntimeError(
            f"Grupos actualizados ({updated_report_count}) distinto al summary "
            f"({summary['number_of_groups']}). Abortando (igual que legacy)."
        )
    updated_report_df.write.mode("overwrite").parquet(f"s3://{ANALYTICS_BUCKET}/{FINAL_PREFIX}/report/")

    detail_df = spark.read.parquet(f"s3://{ANALYTICS_BUCKET}/{STATE_PREFIX}/detail/")
    cost_by_group = updated_report_df.select(
        *GROUP_DIMS,
        "transaction_scheme_fee_cost", "unitary_scheme_fee_cost",
        "estimated_scheme_fee_cost", "unitary_estimated_scheme_fee_cost",
    )
    final_detail_df = detail_df.drop(
        "transaction_scheme_fee_cost", "unitary_scheme_fee_cost",
        "estimated_scheme_fee_cost", "unitary_estimated_scheme_fee_cost",
    ).join(cost_by_group, on=GROUP_DIMS, how="left")

    final_rows = final_detail_df.count()
    if final_rows != summary["number_of_inserted_rows"]:
        raise RuntimeError(
            f"Filas de detalle actualizadas ({final_rows}) distinto al summary "
            f"({summary['number_of_inserted_rows']}). Abortando (igual que legacy)."
        )
    final_detail_df.write.mode("overwrite").parquet(f"s3://{ANALYTICS_BUCKET}/{FINAL_PREFIX}/detail/")

    summary["updated_at"] = datetime.now().isoformat()
    summary["number_of_updated_rows"] = final_rows
    _write_json_to_s3(summary, ANALYTICS_BUCKET, summary_key)
    log_info(f"[run_read] OK — {json.dumps(summary)}")


def run_read():
    update_report_and_propagate()


# =============================================================================
# MAIN
# =============================================================================


if __name__ == "__main__":
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    log_info(f"=== scheme_fee.py — mode={MODE} client={CLIENT_CODE} report_month={REPORT_MONTH} ===")

    if MODE == "generate":
        run_generate()
    elif MODE == "read":
        run_read()
    else:
        raise ValueError(f"--mode debe ser 'generate' o 'read', recibido: {MODE!r}")

    job.commit()
