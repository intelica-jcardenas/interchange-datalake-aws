# =============================================================================
# ITX-INTERCHANGE (PySpark) - AWS Glue Job
# =============================================================================

import sys
import json
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
    log_info(f"  Reading: {path}")
    df = spark.read.parquet(path)
    log_info(f"  → {df.count():,} records")
    return df

def save_parquet(df: DataFrame, path: str):
    """
    Guarda el DataFrame como Parquet.
    coalesce(1) para archivos pequeños, repartition(4) para grandes
    evitando el error RPC message too large.
    """
    count = df.count()
    if count > 200_000:
        log_info(f"  Large file ({count:,} rows) — using repartition(4)")
        df.repartition(4).write.mode("overwrite").parquet(path)
    else:
        df.coalesce(1).write.mode("overwrite").parquet(path)
    log_info(f"  Saved: {path}")

# =============================================================================
# CARGA DE TABLAS DE REFERENCIA
# =============================================================================

def load_visa_rules(reference_bucket: str, file_date: date) -> pd.DataFrame:
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
    date_str = file_date.strftime("%Y-%m-%d")
    path = f"s3://{reference_bucket}/exchange_rate/"
    log_info(f"Loading exchange_rates from: {path} for date: {date_str}")

    df_spark = spark.read.parquet(path) \
        .filter(F.col("rate_date") == date_str) \
        .filter(F.upper(F.col("brand")) == brand.upper())

    df = df_spark.toPandas()

    if not df.empty:
        df["exchange_value"] = pd.to_numeric(df["exchange_value"], errors="coerce")

    log_info(f"exchange_rates loaded: {len(df):,} rates")
    return df

# =============================================================================
# RENOMBRADO DE REGLAS SEGÚN TYPE_RECORD
# =============================================================================

def _rename_rules(rules_pd: pd.DataFrame, type_record: str) -> pd.DataFrame:
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
            "transaction_code_qualifier", "type_purchase"
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
    Para cada moneda target en las reglas con condición de monto,
    crea columna source_amount_{currency} con el monto convertido.
    Ej: source_amount_eur = source_amount * exchange_rate(USD→EUR)
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
    "source_currency_code_alphabetic", "cashback", "message_identifier",
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


def _apply_default(
    condition_name: str,
    condition_value: str,
    batch: pd.DataFrame
) -> pd.DataFrame:
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


def _apply_amount_currency(
    condition_name: str,
    condition_value: str,
    rule: pd.Series,
    batch: pd.DataFrame,
    rates_pd: pd.DataFrame
) -> pd.DataFrame:
    """
    Condición de monto con conversión de moneda.
    Fix: preservar índice original antes del merge para evitar KeyError.
    """
    target_currency = str(rule.get("source_currency_code_alphabetic", "")).strip().upper()
    if not target_currency or target_currency in ("", "NAN", "NONE"):
        return batch

    target_rates = rates_pd[rates_pd["currency_to"].str.upper() == target_currency]

    # ✅ Preservar índice original antes del merge
    # pd.merge resetea los índices → batch.loc[filter_df.index] falla
    batch_reset = batch.reset_index()  # índice original pasa a columna "index"

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

    # ✅ Recuperar filas originales usando el índice preservado
    matched_original_indices = filter_df["index"].tolist()
    return batch.loc[matched_original_indices]


def _apply_condition_pandas(
    condition_name: str,
    rule: pd.Series,
    batch: pd.DataFrame,
    rates_pd: pd.DataFrame
) -> pd.DataFrame:
    condition_value = str(rule[condition_name]).strip()
    if condition_value.upper() in ("", "NAN", "NONE"):
        return batch

    if condition_name in COLUMN_GROUP_GREATER_LESS:
        return _apply_greater_less(condition_name, condition_value, batch)
    elif condition_name in COLUMN_GROUP_AMOUNT_CURRENCY:
        return _apply_amount_currency(condition_name, condition_value, rule, batch, rates_pd)
    else:
        return _apply_default(condition_name, condition_value, batch)


def _evaluate_rules_pandas(
    transactions_pd: pd.DataFrame,
    rules_pd: pd.DataFrame,
    rates_pd: pd.DataFrame
) -> pd.DataFrame:
    """
    Evalúa reglas first-match-wins por jurisdicción.
    Lógica idéntica al código local original.
    Llamada desde mapInPandas — recibe un chunk de transacciones.
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

            #Yield SOLO las columnas esenciales — no pasar las 252 columnas
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
    # CAL columns (ARDEF-derived computed fields) take precedence over CLN raw Visa fields.
    # Rename overlapping CLN columns with _cln suffix so no data is lost and CAL version is used.
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

    # source_amount y source_currency_code_alphabetic se usaron en calculate_fee_amounts
    # pero no forman parte del output ITX — ya existen en el CLN y el store los toma de ahí.
    # Alineado con el prototipo local: stage.drop(["source_currency_code_alphabetic", "source_amount"])
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