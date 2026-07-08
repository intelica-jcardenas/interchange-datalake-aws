
import os
from decimal import Decimal
from typing import Any, List
from functools import reduce

from datetime import datetime, timedelta

import boto3
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

# Solo necesario para SBSA si quieres aplicar hash_file_filter.
# Debe apuntar al parquet equivalente de control.t_control_file.
CONTROL_FILE_PATH = os.getenv("CONTROL_FILE_PATH", "")


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
COL_TRX_RATE_CURRENCY = "rate_currency"


# =============================================================================
# HELPERS
# =============================================================================

def read_ipm_1644_operational(spark, path: str) -> DataFrame:
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
            DecimalType(6, 2),
            True,
        ),
        StructField(
            "amount_net_transaction_in_reconciliation_currency_2_pds_394_2",
            DecimalType(9, 2),
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

        StructField("amount_reconciliation_de_5", DecimalType(8, 4), True),
        StructField("amount_transaction_de_4", DecimalType(10, 4), True),
        StructField("amounts_transaction_fee_7_pds_146_7", DecimalType(6, 4), True),

        StructField("currency_code_reconciliation_de_50", LongType(), True),
        StructField("currency_code_transaction_de_49", LongType(), True),

        StructField("jurisdiction", StringType(), True),
        StructField("settlement_report_currency_code", StringType(), True),
        StructField("intelica_id", StringType(), True),
        StructField("calculated_value", DoubleType(), True),
        StructField("rate_currency", StringType(), True),
    ])
    
    print(f"[READ] IPM_1240 with explicit schema: {path}", flush=True)

    return (
        spark.read
        .schema(schema)
        .option("mergeSchema", "false")
        .parquet(path)
    )
    
def clean_decimal(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    return value


def get_client_from_dynamo(customer_code: str) -> dict:
    ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = ddb.Table(DYNAMO_TABLE_CLIENT)

    resp = table.get_item(Key={"client_id": customer_code})
    item = resp.get("Item")

    if not item:
        raise ValueError(f"No existe cliente en DynamoDB: {customer_code}")

    return {k: clean_decimal(v) for k, v in item.items()}


def read_parquet(spark, path: str, name: str) -> DataFrame:
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
    if col_name in df.columns:
        return F.col(col_name)
    return F.lit(default)


def yymmdd_to_date(col_expr):
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

def get_control_hash_codes(
    spark,
    customer_code: str,
    query_date: str,
    brand_local_indicator: str,
    include_cps_collections: bool = False,
) -> List[str]:
    """
    Replica la lógica PostgreSQL para SBSA.

    Settlement:
      brand: MasterCard_Inward / MasterCard_Outward
      default: Local_MasterCard

    Transactional:
      brand: MasterCard_Inward / MasterCard_Outward / MasterCard_CPS_Collections_File
      default: Local_MasterCard

    Si no hay CONTROL_FILE_PATH, retorna ['xyz'] como fallback,
    igual que el SQL original.
    """

    if not CONTROL_FILE_PATH:
        print("[WARN] CONTROL_FILE_PATH no configurado. hash_file_filter usará ['xyz'].")
        return ["xyz"]

    df_control = read_parquet(spark, CONTROL_FILE_PATH, "control_file")

    base = df_control.filter(
        (F.col("customer") == customer_code)
        & (F.col("brand") == "MC")
        & (F.col("file_date").cast("date") == F.lit(query_date).cast("date"))
    )

    if brand_local_indicator.lower() == "brand":
        cond = (
            F.col("process_file_name").like("%MasterCard_Inward%")
            | F.col("process_file_name").like("%MasterCard_Outward%")
        )

        if include_cps_collections:
            cond = cond | F.col("process_file_name").like("%MasterCard_CPS_Collections_File%")

        base = base.filter(cond)
    else:
        base = base.filter(F.col("process_file_name").like("%Local_MasterCard%"))

    codes = [
        r["code"]
        for r in base.select("code").distinct().collect()
        if r["code"]
    ]

    return codes if codes else ["xyz"]


# =============================================================================
# VALIDATION CONDITIONS
# =============================================================================

def apply_validation_conditions(
    spark,
    df: DataFrame,
    df_validation: DataFrame,
    query_date: str,
    customer_code: str,
    issuer_acquirer_indicator: str,
    brand_local_indicator: str,
    data_source: str,
    member_activity_col: str | None = None,
    hash_col: str = "content_hash",
) -> DataFrame:
    """
    Aplica condiciones dinámicas desde validation_conditions.

    Se interpretan patrones conocidos del SQL legacy:
      - where con reconciled_member_activity
      - hash_file_filter solo para SBSA
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
            hash_codes = get_control_hash_codes(
                spark=spark,
                customer_code=customer_code,
                query_date=query_date,
                brand_local_indicator=brand_local_indicator,
                include_cps_collections=(data_source == "trx"),
            )

            print(f"[VALIDATION][{data_source}] SBSA hash_file_filter codes={len(hash_codes)}")
            df = df.filter(F.col(actual_hash_col).isin(hash_codes))

    return df


# =============================================================================
# FUNCIÓN SETTLEMENT
# =============================================================================

def get_mastercard_validation_results_settlement(
    spark,
    query_date: str,
    customer_code: str,
    issuer_acquirer_indicator: str,
    brand_local_indicator: str = "default",
) -> DataFrame:
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

    btt_path = f"{S3_REFERENCE}/mastercard_business_transaction_type/"
    currency_path = f"{S3_REFERENCE}/currency/"
    validation_path = f"{S3_REFERENCE}/validation_conditions/"

    df_raw = read_ipm_1644_operational(spark, data_path)

    df_exchange = read_exchange_rates_glue(spark, S3_REFERENCE, query_date)
    df_btt = read_parquet(spark, btt_path, "mastercard_business_transaction_type")
    df_currency = read_parquet(spark, currency_path, "currency")
    df_validation = read_parquet(spark, validation_path, "validation_conditions")

    
    t1 = (
        df_raw
        .filter(F.col("type_mti").cast("string") == "1644")
        .filter(F.col("function_code").cast("string") == "685")
        .filter(F.col(COL_STL_TXN_FUNC).cast("string") == "1240")
    )


    t1 = apply_validation_conditions(
        spark=spark,
        df=t1,
        df_validation=df_validation,
        query_date=query_date,
        customer_code=customer_code,
        issuer_acquirer_indicator=issuer_acquirer_indicator,
        brand_local_indicator=brand_local_indicator,
        data_source="stl",
        member_activity_col=COL_STL_MEMBER_ACTIVITY,
        hash_col="content_hash",
    )
    if DEBUG:
        print(f"[COUNT][STL] filtered_transactions={t1.count()}")


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
        safe_col(t1, COL_STL_MSG_REASON, "").cast("string").alias("message_reason_code"),
        safe_col(t1, COL_STL_ECI, "").cast("string").alias("electronic_commerce_indicator_3"),
        F.col(COL_STL_PROCESSING_CODE).cast("string").alias("reconciled_processing_code"),
        F.col(COL_STL_CURRENCY_RECON).cast("string").alias("currency_code_reconciliation"),
        F.col("function_code").cast("string").alias("function_code"),
        F.col(COL_STL_TXN_FUNC).cast("string").alias("reconciled_transaction_function_1"),
        F.col(COL_STL_SETTLEMENT_INDICATOR).cast("string").alias("settlement_indicator_1"),
        F.col("type_mti").cast("string").alias("app_message_type"),
        safe_col(t1, COL_STL_RECONCILED_FILE, "").cast("string").alias("reconciled_file"),
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
) -> DataFrame:
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

    btt_path = f"{S3_REFERENCE}/mastercard_business_transaction_type/"
    currency_path = f"{S3_REFERENCE}/currency/"
    validation_path = f"{S3_REFERENCE}/validation_conditions/"

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

    t1 = df_raw.filter(F.col(COL_TRX_MTI).cast("string") == "1240")

    t1 = apply_validation_conditions(
        spark=spark,
        df=t1,
        df_validation=df_validation,
        query_date=query_date,
        customer_code=customer_code,
        issuer_acquirer_indicator=issuer_acquirer_indicator,
        brand_local_indicator=brand_local_indicator,
        data_source="trx",
        member_activity_col=None,
        hash_col=COL_TRX_HASH,
    )
    if DEBUG:
        print(f"[COUNT][TRX] filtered_transactions={t1.count()}")

    # iqa_processing_code:
    # En SQL legacy puede venir una expresión custom por banco.
    # Por defecto PostgreSQL usa LEFT(processing_code, 2).
    t1_norm = t1.select(
        yymmdd_to_date(F.col(COL_TRX_FILE_DATE)).alias("app_processing_date"),
        F.col(COL_TRX_HASH).cast("string").alias("app_hash_file"),
        F.col(COL_TRX_APP_ID).cast("long").alias("app_id"),
        F.lit(customer_code).cast("string").alias("app_customer_code"),
        F.col(COL_TRX_MTI).cast("string").alias("app_message_type"),
        F.lit(file_type).cast("string").alias("app_type_file"),

        F.col(COL_TRX_SETTLEMENT_INDICATOR).cast("string").alias("settlement_indicator_1"),
        F.substring(F.col(COL_TRX_PROCESSING_CODE).cast("string"), 1, 2).alias("processing_code"),
        F.col(COL_TRX_BUSINESS_ACTIVITY_4).cast("string").alias("business_activity_4"),

        safe_col(t1, COL_TRX_MSG_REASON, "").cast("string").alias("message_reason_code"),
        safe_col(t1, COL_TRX_ECI, "").cast("string").alias("electronic_commerce_indicator_3"),

        F.col(COL_TRX_AMOUNT_RECON).cast("double").alias("amount_reconciliation"),
        F.col(COL_TRX_AMOUNT_TRANSACTION).cast("double").alias("amount_transaction"),
        F.col(COL_TRX_FEE_7).cast("double").alias("amounts_transaction_fee_7"),

        F.col(COL_TRX_CURRENCY_RECON).cast("string").alias("currency_code_reconciliation"),
        F.col(COL_TRX_CURRENCY_TRANSACTION).cast("string").alias("currency_code_transaction"),

        F.col(COL_TRX_JURISDICTION).cast("string").alias("jurisdiction"),
        F.col(COL_TRX_SETTLEMENT_REPORT_CURRENCY).cast("string").alias("settlement_report_currency_code"),

        F.col(COL_TRX_INTELICA_ID).cast("string").alias("intelica_id"),
        F.col(COL_TRX_CALCULATED_VALUE).cast("double").alias("calculated_value"),
        F.col(COL_TRX_RATE_CURRENCY).cast("string").alias("rate_currency"),
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
        x2_currency_join = "X2.currency_from_code = T1.currency_code_reconciliation"
    else:
        trx_amt_expr = "T1.amount_transaction"
        itx_amt_expr = "T1.calculated_value"
        x1_currency_join = "X1.currency_from_code = T1.currency_code_transaction"
        x2_currency_join = "X2.currency_from = T1.rate_currency"

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
                SUM(COALESCE(X2.exchange_value, X1.exchange_value, 1) * {itx_amt_expr})
                AS DECIMAL(38,6)
            )                                                           AS itx_amt

        FROM trx_t1 T1

        LEFT JOIN trx_m1 M1
            ON T1.processing_code = M1.business_transaction_type_id

        LEFT JOIN trx_x X1
            ON {x1_currency_join}
           AND X1.currency_to = CAST('{report_currency_code}' AS STRING)

        LEFT JOIN trx_x X2
            ON {x2_currency_join}
           AND X2.currency_to = CAST('{report_currency_code}' AS STRING)

        LEFT JOIN trx_mc MC
            ON MC.currency_alphabetic_code = T1.settlement_report_currency_code

        WHERE T1.app_message_type = '1240'

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
) -> DataFrame:

    df_settlement = get_mastercard_validation_results_settlement(
        spark=spark,
        query_date=query_date,
        customer_code=customer_code,
        issuer_acquirer_indicator=issuer_acquirer_indicator,
        brand_local_indicator=brand_local_indicator,
    )

    df_transactional = get_mastercard_validation_results_transactional(
        spark=spark,
        query_date=query_date,
        customer_code=customer_code,
        issuer_acquirer_indicator=issuer_acquirer_indicator,
        brand_local_indicator=brand_local_indicator,
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

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

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
    else:
        output_path = (
            f"{S3_OUTPUT}/{customer_code}/reports/quality/"
            f"range_{start_date}_{end_date}/"
        )

    (
        df_result
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"[OK] Output combinado generado en: {output_path}", flush=True)

    spark.stop()
