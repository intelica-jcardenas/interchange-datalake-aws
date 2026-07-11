"""
get_transaction.py — Job real: itl-0004-itx-dev-intchg-02-glue-get-transaction
================================================================================
Archivo:     glue/scripts/reports/get_transaction/get_transaction.py
S3 Script:   s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/get_transaction.py

Genera el reporte consolidado de transacciones de un cliente (Visa BASE II,
Visa SMS y Mastercard MTI 1240/1442) a partir de los Parquets finales del
bucket operational del Data Lake, en un único schema unificado de 32
columnas listo para análisis en Athena.

Fuentes de datos
----------------
  S3 operational  : Visa BASEII, Visa SMS, MC IPM_1240, MC IPM_1442
  S3 reference    : country/data.parquet, currency/data.parquet,
                    exchange-rates-glue/brand=*/exchange_date=*/, visa_bin_products/data.parquet,
                    mastercard_bin_products/data.parquet
  DynamoDB        : tabla client (report_currency_code, duplicate_on_us_flag_*)

Salida
------
  S3 analytics    : {client_id}/reports/report_transactions_{client_id}_{report_suffix}.parquet
  Una sola escritura por ejecución, cubriendo el rango [start_date, end_date].
  report_suffix es un parámetro del job (ej: "202601", "202601_v2").

Schema de salida (32 columnas)
-------------------------------
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
  --scheme_fee            "true" / "false" — activa el duplicado on-us y el
                           join de costo de cuotas contra scheme_fee.py
  --operational_bucket    Nombre del bucket operational (lectura de Parquets)
  --reference_bucket      Nombre del bucket reference (country, exchange-rates)
  --analytics_bucket      Nombre del bucket analytics (escritura del reporte)
  --dynamodb_table_client Nombre de la tabla DynamoDB de clientes

Pendientes (TODO)
-----------------
  - Visa SMS: implementado y validado individualmente, pero deshabilitado
    a propósito en process_client_range() mientras los bancos activos
    (EBGR, SBSA) no manejan ese tipo de transacción.
  - scheme_fees_amount: validado estructuralmente con datos reales pero
    solo con costos de prueba — falta validar con el costo real que
    devuelve el equipo externo de cuotas.
"""

import sys
import boto3

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
)
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
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)

# =============================================================================
# Logs definition
# =============================================================================
logger = glueContext.get_logger()


def log_info(message: str):
    """
    Escribe un mensaje informativo al log de CloudWatch del job, con el
    prefijo estándar que usa el logger de Glue.

    Args:
        message: Texto a loggear. Ejemplo: "[read_operational] Loaded 4200 rows".
    """
    logger.info(f"GlueLogger: {message}")


def log_error(message: str):
    """
    Escribe un mensaje de error al log de CloudWatch del job. No detiene
    la ejecución — se usa junto con try/except para que el fallo de un
    tramo del reporte (ej. un brand sin datos) no aborte el job completo.

    Args:
        message: Texto a loggear. Ejemplo: "[BASEII] SBSA/2026-01-01_2026-01-31: ...".
    """
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
    """
    Lee la configuración de un cliente desde la tabla DynamoDB client-02:
    la moneda en la que se debe reportar y si corresponde duplicar sus
    transacciones "on-us" (mismo banco como emisor y adquirente).

    Los flags llegan de DynamoDB como texto, no como booleano — se parsean
    explícitamente comparando contra "TRUE" (case-insensitive) en vez de
    usar bool() directo, porque bool("FALSE") es True en Python y castearía
    cualquier flag no vacío a True sin importar su contenido real.
    report_currency_code usa "or" en vez del default de .get() porque ese
    default no aplica cuando la clave existe pero está vacía.

    Args:
        client_code: Código de cliente. Ejemplo: "SBSA", "EBGR".

    Returns:
        dict con:
          report_currency (str): moneda de reporte, ej. "ZAR". "USD" si el
            cliente no tiene el campo configurado.
          dup_on_us_visa (bool): si True, duplicar transacciones on-us Visa.
          dup_on_us_mc (bool): si True, duplicar transacciones on-us Mastercard.

    Ejemplo:
        cfg = get_client_config("SBSA")
        # {"report_currency": "ZAR", "dup_on_us_visa": True, "dup_on_us_mc": True}
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DDB_CLIENT_TABLE)
    resp = table.get_item(Key={"client_id": client_code})
    item = resp.get("Item", {})

    def _flag(value) -> bool:
        return str(value).strip().upper() == "TRUE"

    return {
        "report_currency": item.get("report_currency_code") or "USD",
        "dup_on_us_visa": _flag(item.get("duplicate_on_us_flag_visa", "")),
        "dup_on_us_mc": _flag(item.get("duplicate_on_us_flag_mastercard", "")),
    }


def read_operational(
    client_id: str, brand: str, data_type: str, start_date: str, end_date: str
) -> DataFrame | None:
    """
    Lee todos los Parquets del bucket operational para un cliente, marca y
    tipo de dato dentro del rango de fechas pedido, y agrega la columna
    customer_code con el literal del cliente.

    "content_hash" ya viene como columna real en el Parquet (Visa y
    Mastercard) — se deja tal cual acá, cada transform la alias a "file_id"
    en su propio .select() final. No se deriva "file_id" del nombre del
    archivo: en Visa el archivo se llama "{content_hash}.parquet" (coincide
    por casualidad), pero en Mastercard se llama
    "{file_id}_{bloque}_{mti}.parquet" — el nombre completo del archivo no
    es el content_hash ahí.

    Las particiones Hive (file_type, date) quedan disponibles como columnas
    del DataFrame automáticamente, sin necesidad de parsearlas del path.

    Args:
        client_id: Código de cliente. Ejemplo: "SBSA".
        brand: "VISA" o "MC" — primer nivel del path en el bucket operational.
        data_type: Tipo de dato dentro de la marca. Ejemplo: "baseii_drafts",
            "sms_messages", "IPM_1240", "IPM_1442".
        start_date: Fecha de inicio del rango, formato "YYYY-MM-DD".
        end_date: Fecha de fin del rango, formato "YYYY-MM-DD".

    Returns:
        DataFrame con todas las columnas del operational + customer_code, o
        None si no hay Parquets en ese path o no hay filas en el rango.

    Ejemplo:
        df = read_operational("SBSA", "VISA", "baseii_drafts",
                               "2026-01-01", "2026-01-31")
    """
    base_path = f"s3://{OPERATIONAL_BUCKET}/{client_id}/{brand}/{data_type}/"

    try:
        df = spark.read.parquet(base_path)
    except Exception as e:
        log_info(f"[read_operational] No data at {base_path}: {e}")
        return None

    df = df.filter(F.col("date").between(start_date, end_date))

    if df.rdd.isEmpty():
        log_info(
            f"[read_operational] No rows for {client_id}/{brand}/{data_type} "
            f"in [{start_date}, {end_date}]"
        )
        return None

    df = df.withColumn("customer_code", F.lit(client_id))

    log_info(
        f"[read_operational] Loaded {client_id}/{brand}/{data_type}: {df.count()} rows"
    )
    return df


# =============================================================================
# TABLAS DE REFERENCIA
# =============================================================================


def load_country() -> DataFrame:
    """
    Carga la tabla de referencia de países desde el bucket S3 reference.
    Se usa para traducir el código de país de 2 letras que trae el
    operational de Visa a su equivalente de 3 letras del reporte final.

    Returns:
        DataFrame con country_code (2 letras, ej. "GR") y
        country_code_alternative (3 letras, ej. "GRC").

    Ejemplo:
        country_df = load_country()
    """
    path = f"s3://{BUCKET_REF}/country/data.parquet"
    return spark.read.parquet(path).select("country_code", "country_code_alternative")


def load_visa_bin_products() -> DataFrame:
    """
    Carga la tabla de referencia de productos Visa por rango de BIN desde
    el bucket S3 reference. Se usa para resolver product_program_id a
    partir del product_id que ya viene calculado en el Parquet operational.

    Returns:
        DataFrame con bin_product_id y range_program_id.

    Ejemplo:
        vi_bin_products_df = load_visa_bin_products()
    """
    path = f"s3://{BUCKET_REF}/visa_bin_products/data.parquet"
    return spark.read.parquet(path).select("bin_product_id", "range_program_id")


def load_currency() -> DataFrame:
    """
    Carga la tabla de referencia de monedas (código ISO 4217 numérico →
    alfabético) desde el bucket S3 reference. Se usa para traducir los
    códigos de moneda numéricos del operational de Mastercard
    (currency_code_transaction_de_49 / currency_code_reconciliation_de_50)
    al código alfabético que usan las columnas from_currency/to_currency
    de la tabla de tipo de cambio.

    Returns:
        DataFrame con currency_numeric_code y currency_alphabetic_code.

    Ejemplo:
        currency_df = load_currency()
    """
    path = f"s3://{BUCKET_REF}/currency/data.parquet"
    return spark.read.parquet(path).select(
        "currency_numeric_code", "currency_alphabetic_code"
    )


def load_mastercard_bin_products() -> DataFrame:
    """
    Carga la tabla de referencia de productos Mastercard por rango de BIN
    desde el bucket S3 reference. Se usa para resolver product_program_id
    a partir de gcms_product_identifier (DE_48 PDS_0002), ya presente en
    el Parquet operational.

    Returns:
        DataFrame con bin_product_id y range_program_id.

    Ejemplo:
        mc_bin_products_df = load_mastercard_bin_products()
    """
    path = f"s3://{BUCKET_REF}/mastercard_bin_products/data.parquet"
    return spark.read.parquet(path).select("bin_product_id", "range_program_id")


def load_exchange_rates(start_date: str, end_date: str, brand_path: str) -> DataFrame:
    """
    Carga los tipos de cambio del bucket S3 reference para el rango de
    fechas pedido, desde exchange-rates-glue/ — la fuente de tipo de
    cambio con cobertura viva del pipeline, particionada por marca y
    fecha, con códigos de moneda numéricos y alfabéticos ya combinados.

    Si no hay datos en el path (marca o rango sin cobertura), retorna un
    DataFrame vacío con el schema correcto en vez de fallar, para que el
    resto del job siga funcionando con tasas por defecto (1.0).

    Args:
        start_date: Fecha de inicio del rango, formato "YYYY-MM-DD".
        end_date: Fecha de fin del rango, formato "YYYY-MM-DD".
        brand_path: Marca para el filtro de partición. Ejemplo: "Visa",
            "MasterCard" (se capitaliza automáticamente).

    Returns:
        DataFrame con exchange_date, from_currency, to_currency, fx_rate.

    Ejemplo:
        xrate_vi_df = load_exchange_rates("2026-01-01", "2026-01-31", "Visa")
    """
    brand_partition = brand_path.capitalize()
    path = f"s3://{BUCKET_REF}/exchange-rates-glue/brand={brand_partition}/"
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        log_error(f"[load_exchange_rates] No exchange rates at {path}: {e}")
        return spark.createDataFrame(
            [],
            schema="exchange_date string, from_currency string, to_currency string, fx_rate double",
        )

    df = df.select(
        F.col("exchange_date"),
        F.col("from_currency"),
        F.col("to_currency"),
        F.col("fx_rate"),
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
    """
    Invierte el signo de transaction_amount e interchange_fees_amount en
    las filas marcadas como reversa/contracargo, para que el reporte
    refleje el efecto neto real de la transacción sobre el balance del
    cliente. scheme_fees_amount no se toca — el costo de cuota no se
    revierte.

    Args:
        df: DataFrame ya con las columnas transaction_amount,
            interchange_fees_amount e is_reversal_or_chargeback.

    Returns:
        Mismo DataFrame con los 2 montos negados donde corresponde.

    Ejemplo:
        df = _apply_reversal_negation(df)
        # Una fila con is_reversal_or_chargeback=True y transaction_amount=100.0
        # queda con transaction_amount=-100.0.
    """
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
    Duplica las transacciones "on-us" (donde el mismo banco es emisor y
    adquirente de la tarjeta) agregando una copia marcada como emisor,
    para que el reporte final refleje ambos roles de negocio por
    separado. Solo se llama cuando el cliente tiene esta duplicación
    activada (ver client_cfg["dup_on_us_visa"]/["dup_on_us_mc"]) y el job
    corre con --scheme_fee=true.

    Args:
        df: DataFrame ya transformado (con columnas jurisdiction_code y
            business_mode_code), antes de unir con otros brands.
        brand: Solo para logging/trazabilidad, no afecta la lógica.
            Ejemplo: "VISA", "MC".

    Returns:
        df original + las filas duplicadas (union), mismo schema.

    Ejemplo:
        df = _apply_duplicate_on_us(df, brand="VISA")
        # Una transacción on-us con business_mode_code='A' (adquirente)
        # genera una segunda fila idéntica salvo business_mode_code='I'.
    """
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
    Agrega al DataFrame las tasas de cambio necesarias para convertir
    montos a la moneda de reporte del cliente: xr1_rate (moneda de la
    transacción → report_currency) y xr2_rate (moneda del fee de
    interchange → report_currency). El join es por fecha de procesamiento
    exacta contra exchange_date.

    Args:
        df: DataFrame con las columnas "date", src_currency_col y
            fee_currency_col ya resueltas.
        xrate_df: Tabla de tipo de cambio (via load_exchange_rates()).
        src_currency_col: Nombre de la columna con la moneda de la
            transacción. Ejemplo: "source_currency_code_alphabetic".
        fee_currency_col: Nombre de la columna con la moneda del fee de
            interchange. Ejemplo: "interchange_fee_currency".
        report_currency: Moneda de reporte del cliente. Ejemplo: "ZAR".

    Returns:
        df + columnas xr1_rate y xr2_rate (NULL si no hubo match — se
        maneja con coalesce(...,1.0) en el punto de uso).

    Ejemplo:
        df = _join_exchange_rates(df, xrate_vi_df,
                                   "source_currency_code_alphabetic",
                                   "interchange_fee_currency", "ZAR")
    """
    xr = xrate_df.filter(F.col("to_currency") == report_currency)

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
    Transforma las transacciones Visa BASE II del bucket operational al
    schema estándar de reporte de transacciones.

    Enriquece cada transacción cruzando contra tablas de referencia
    (país del comercio, país del emisor vía el rango ARDEF, producto →
    rango de programa) y convierte los montos a la moneda de reporte del
    cliente usando el tipo de cambio del día. El resultado queda listo
    para unirse con las transformaciones de SMS y Mastercard en un único
    reporte consolidado.

    Args:
        df: Parquet operational de baseii_drafts (via read_operational()),
            ya filtrado por rango de fechas. Debe incluir content_hash,
            record, business_mode, draft_code, ardef_country, product_id.
        country_df: Tabla de referencia de países (via load_country()).
        xrate_df: Tipo de cambio (via load_exchange_rates()).
        vi_bin_products_df: Tabla de productos Visa por BIN (via
            load_visa_bin_products()).
        report_currency: Moneda de reporte del cliente. Ejemplo: "ZAR".
        dup_on_us: Si True, duplica transacciones on-us adquirentes
            agregando una copia como emisor (ver _apply_duplicate_on_us()).

    Returns:
        DataFrame con las 32 columnas de FINAL_COLS, listo para unir con
        transform_visa_sms()/transform_mastercard() vía unionByName.

    Ejemplo:
        df_baseii = read_operational("SBSA", "VISA", "baseii_drafts",
                                      "2026-01-01", "2026-01-31")
        result = transform_visa_baseii(df_baseii, country_df, xrate_vi_df,
                                        vi_bin_products_df, "ZAR",
                                        dup_on_us=True)
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

    df = _join_exchange_rates(
        df,
        xrate_df,
        src_currency_col="source_currency_code_alphabetic",
        fee_currency_col="interchange_fee_currency",
        report_currency=report_currency,
    )

    df = df.select(
        F.col("customer_code").cast(StringType()),
        F.col("content_hash").cast(StringType()).alias("file_id"),
        F.col("record").cast(IntegerType()).alias("row_id"),
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
        (F.coalesce(F.col("xr1_rate"), F.lit(1.0)) * F.col("source_amount"))
        .cast(DoubleType())
        .alias("transaction_amount"),
        (
            F.coalesce(F.col("xr1_rate"), F.lit(1.0))
            * F.col("interchange_fee_amount_itx")
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
    Transforma las transacciones Visa SMS (mensajería de cajeros
    automáticos) del bucket operational al schema estándar de reporte de
    transacciones. Mismo patrón de enriquecimiento y conversión de moneda
    que transform_visa_baseii(), con una regla de monto adicional para el
    caso source_amount=0 (ver más abajo, xr3).

    Args:
        df: Parquet operational de sms_messages (via read_operational()),
            ya filtrado por rango de fechas.
        country_df: Tabla de referencia de países (via load_country()).
        xrate_df: Tipo de cambio (via load_exchange_rates()).
        vi_bin_products_df: Tabla de productos Visa por BIN (via
            load_visa_bin_products()).
        report_currency: Moneda de reporte del cliente. Ejemplo: "ZAR".
        dup_on_us: Si True, duplica transacciones on-us adquirentes
            agregando una copia como emisor.

    Returns:
        DataFrame con las 32 columnas de FINAL_COLS.

    Ejemplo:
        df_sms = read_operational("SBSA", "VISA", "sms_messages",
                                   "2026-01-01", "2026-01-31")
        result = transform_visa_sms(df_sms, country_df, xrate_vi_df,
                                     vi_bin_products_df, "ZAR",
                                     dup_on_us=True)
    """
    df = df.filter(F.col("local_draft_date").isNotNull())

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
        df["card_acceptor_country"] == merchant_country_ref["_country_code_merchant"],
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

    df = _join_exchange_rates(
        df,
        xrate_df,
        src_currency_col="source_currency_code_alphabetic",
        fee_currency_col="interchange_fee_currency",
        report_currency=report_currency,
    )

    # xr3: tasa USD -> report_currency (fija). Para el caso source_amount=0,
    # cryptogram_amount y surcharge_amount_sms siempre vienen en USD segun la
    # especificacion de Visa, sin importar la moneda real de la transaccion,
    # asi que la conversion usa siempre USD como origen (no la moneda
    # reportada en cryptogram_currency_code -- probado y descartado: producia
    # montos ~16x menores para business_transaction_type=247, ATM balance
    # inquiry, MCC 6011, en la validacion contra SBSA enero 2026).
    xr3 = xrate_df.filter(
        (F.col("to_currency") == report_currency) & (F.col("from_currency") == "USD")
    ).select(
        F.col("exchange_date").alias("_xr3_date"),
        F.col("fx_rate").alias("xr3_rate"),
    )
    df = df.join(xr3, df["date"] == xr3["_xr3_date"], how="left").drop("_xr3_date")

    df = df.select(
        F.col("customer_code").cast(StringType()),
        F.col("content_hash").cast(StringType()).alias("file_id"),
        F.col("record").cast(IntegerType()).alias("row_id"),
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
        F.col("local_draft_date").cast(DateType()).alias("transaction_date"),
        F.date_format(
            F.col("local_draft_date").cast(DateType()), "yyyy-MM"
        ).alias("transaction_month"),
        F.when(F.col("transaction_code_sms").isin("05", "25"), F.lit("PUR"))
        .when(F.col("transaction_code_sms").isin("06", "26"), F.lit("CRD"))
        .when(F.col("transaction_code_sms").isin("07", "27"), F.lit("CSH"))
        .otherwise(F.lit("OTH"))
        .alias("transaction_group_code"),
        F.col("business_transaction_type").cast(StringType()).alias("transaction_type_id"),
        F.col("jurisdiction").cast(StringType()).alias("jurisdiction_code"),
        F.col("merchant_country_alt").cast(StringType()).alias("merchant_country_code"),
        F.col("`merchant's_type`").cast(IntegerType()).alias("mcc_code"),
        F.trim(F.upper(F.col("card_acceptor_name"))).cast(StringType()).alias("merchant"),
        F.trim(F.upper(F.col("card_acceptor_id_sms"))).cast(StringType()).alias("merchant_id"),
        F.trim(F.upper(F.col("card_acceptor_terminal_id"))).cast(StringType()).alias("terminal_id"),
        F.col("acquiring_institution_id_1").cast(IntegerType()).alias("acquirer_bin"),
        F.col("issuer_country_alt").cast(StringType()).alias("issuer_country_code"),
        F.col("card_number").substr(1, 6).cast(IntegerType()).alias("issuer_bin_6"),
        F.col("issuer_bin_8").cast(IntegerType()).alias("issuer_bin_8"),
        F.col("funding_source").cast(StringType()).alias("funding_source_code"),
        F.col("product_program_id_raw").cast(IntegerType()).alias("product_program_id"),
        F.col("product_id").cast(StringType()).alias("product_code"),
        F.when(F.col("mail_telephone_or_electronic_commerce_indicator") == " ", F.lit("CPR"))
        .when(
            F.col("mail_telephone_or_electronic_commerce_indicator").rlike("^[1-9]$"),
            F.lit("CNP"),
        )
        .otherwise(F.lit("UNK"))
        .alias("card_present_code"),
        F.col("reversal_indicator").cast(BooleanType()).alias("is_reversal_or_chargeback"),
        F.col("interchange_fee_descriptor").cast(StringType()).alias("interchange_rule"),
        F.lit(report_currency).alias("reported_currency_code"),
        F.when(
            F.col("source_amount") == 0,
            F.coalesce(F.col("xr3_rate"), F.lit(1.0))
            * (
                F.coalesce(F.col("cryptogram_amount"), F.lit(0.0))
                + F.coalesce(F.col("surcharge_amount_sms"), F.lit(0.0))
            ),
        )
        .otherwise(F.coalesce(F.col("xr1_rate"), F.lit(1.0)) * F.col("source_amount"))
        .cast(DoubleType())
        .alias("transaction_amount"),
        (
            F.coalesce(F.col("xr1_rate"), F.lit(1.0))
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
    Transforma las transacciones Mastercard (MTI 1240 y 1442) del bucket
    operational al schema estándar de reporte de transacciones.

    A diferencia de Visa, el operational de Mastercard ya trae los países
    en formato alpha-3 (card_acceptor_country_code_de_43_6 para el
    comercio, iar_country para el emisor) — no requiere join contra una
    tabla de países. Las monedas sí llegan como código ISO 4217 numérico
    y se traducen a alfabético (currency_df) para el join de tipo de
    cambio; la moneda del fee IAR (rate_currency) ya viene alfabética.
    El producto se resuelve igual que en Visa: gcms_product_identifier →
    bin_product_id → range_program_id.

    Args:
        df: Parquet operational de IPM_1240 o IPM_1442 (via
            read_operational()), ya filtrado por rango de fechas.
        currency_df: Tabla de monedas numérico→alfabético (via load_currency()).
        xrate_df: Tipo de cambio (via load_exchange_rates()).
        mc_bin_products_df: Tabla de productos Mastercard por BIN (via
            load_mastercard_bin_products()).
        report_currency: Moneda de reporte del cliente. Ejemplo: "ZAR".
        dup_on_us: Si True, duplica transacciones on-us adquirentes
            agregando una copia como emisor.

    Returns:
        DataFrame con las 32 columnas de FINAL_COLS.

    Ejemplo:
        df_mc1240 = read_operational("SBSA", "MC", "IPM_1240",
                                      "2026-01-01", "2026-01-31")
        result = transform_mastercard(df_mc1240, currency_df, xrate_mc_df,
                                       mc_bin_products_df, "ZAR",
                                       dup_on_us=True)
    """
    df = df.withColumn(
        "_effective_product_id",
        F.coalesce(
            F.col("licensed_product_identifier_pds_3"),
            F.col("gcms_product_identifier"),
        ),
    )

    product_ref = mc_bin_products_df.select(
        F.col("bin_product_id").alias("_bin_product_id"),
        F.col("range_program_id").alias("product_program_id_raw"),
    )
    df = df.join(
        product_ref,
        df["_effective_product_id"] == product_ref["_bin_product_id"],
        how="left",
    ).drop("_bin_product_id")

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

    df = df.withColumn(
        "_fee_currency_mc",
        F.when(F.col("file_type") == "IN", F.col("src_currency_alpha"))
        .otherwise(F.col("rate_currency")),
    )

    df = _join_exchange_rates(
        df,
        xrate_df,
        src_currency_col="src_currency_alpha",
        fee_currency_col="_fee_currency_mc",
        report_currency=report_currency,
    )
    df = df.drop("_fee_currency_mc")

    is_1442 = F.col("type_mti") == "1442"

    df = df.select(
        F.col("customer_code").cast(StringType()),
        F.col("content_hash").cast(StringType()).alias("file_id"),
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
        F.when(~is_1442, F.col("iar_country"))
        .when(is_1442, F.col("card_acceptor_country_code_de_43_6"))
        .cast(StringType())
        .alias("issuer_country_code"),
        F.col("pan_de_2").substr(1, 6).cast(IntegerType()).alias("issuer_bin_6"),
        F.col("pan_de_2").substr(1, 8).cast(IntegerType()).alias("issuer_bin_8"),
        F.col("funding_source").cast(StringType()).alias("funding_source_code"),
        F.col("product_program_id_raw").cast(IntegerType()).alias("product_program_id"),
        F.col("_effective_product_id").cast(StringType()).alias("product_code"),
        F.when(F.col("pos_entry_mode_de_22").substr(6, 1) == "1", F.lit("CPR"))
        .when(F.col("pos_entry_mode_de_22").substr(6, 1) == "0", F.lit("CNP"))
        .otherwise(F.lit("UNK"))
        .alias("card_present_code"),
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
        (
            F.coalesce(F.col("xr1_rate"), F.lit(1.0))
            * F.when(
                F.col("file_type") == "IN", F.col("amount_reconciliation_de_5")
            ).otherwise(F.col("amount_transaction_de_4"))
        )
        .cast(DoubleType())
        .alias("transaction_amount"),
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


def load_scheme_fee_costs(
    client_id: str, start_date: str, analytics_bucket: str
) -> DataFrame | None:
    """
    Lee el costo de cuotas ya calculado por el proceso de scheme fee para
    un cliente y mes, y arma la llave necesaria para cruzarlo contra el
    reporte de transacciones: file_id + row_id + business_mode_code
    (traducido de business_mode_id ACQUIRING/ISSUING a A/I).

    Usa la columna UNITARIA (unitary_scheme_fee_cost) — no
    transaction_scheme_fee_cost, que replica el TOTAL del grupo en cada
    fila y sobrecontaría el costo si se sumara fila por fila.

    (file_id, row_id, business_mode_code) es única por construcción en
    el detalle de cuotas: cada transacción fuente genera 1 fila, y el
    duplicado on-us agrega como máximo 1 fila adicional con
    business_mode_id ya forzado al valor opuesto — nunca hay 2 filas con
    el mismo business_mode_code para el mismo (file_id, row_id).

    Args:
        client_id: Código de cliente. Ejemplo: "SBSA".
        start_date: Fecha de inicio del rango del job, formato
            "YYYY-MM-DD". El mes de reporte de cuotas (YYYYMM) se deriva
            de acá — el rango start_date/end_date debe caer dentro de un
            único mes calendario al pedir scheme_fees_amount.
        analytics_bucket: Nombre del bucket S3 analytics.

    Returns:
        DataFrame con file_id, row_id, business_mode_code y
        scheme_fees_amount, o None si el detalle de cuotas todavía no
        existe para ese cliente+mes — en ese caso scheme_fees_amount
        queda en el default 0.0 que cada transform ya trae, sin que
        esto se considere un error del job.

    Ejemplo:
        cost_df = load_scheme_fee_costs("SBSA", "2026-01-01",
                                         "itl-...-s3-analytics")
    """
    report_month = start_date[:4] + start_date[5:7]
    path = f"s3://{analytics_bucket}/{client_id}/scheme_fee/final/{report_month}/detail/"
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        log_info(
            f"[load_scheme_fee_costs] No final/detail for {client_id}/{report_month}: {e}"
        )
        return None

    return df.select(
        F.col("app_hash_file").cast(StringType()).alias("file_id"),
        F.col("app_id").cast(IntegerType()).alias("row_id"),
        F.when(F.col("business_mode_id") == "ACQUIRING", F.lit("A"))
        .when(F.col("business_mode_id") == "ISSUING", F.lit("I"))
        .alias("business_mode_code"),
        F.col("unitary_scheme_fee_cost").cast(DoubleType()).alias("scheme_fees_amount"),
    ).filter(F.col("business_mode_code").isNotNull())


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
    Genera el reporte consolidado de un cliente para un rango de fechas:
    lee cada fuente operational disponible (Visa BASE II, Visa SMS,
    Mastercard MTI 1240/1442), la transforma al schema común, y une todo
    en un único DataFrame. Si --scheme_fee=true, además cruza el costo de
    cuotas ya calculado (ver load_scheme_fee_costs()).

    Cada fuente se procesa en un try/except independiente — si una marca
    no tiene datos o falla su transformación, el reporte sigue con las
    demás en vez de abortar el job completo.

    Args:
        client_id: Código de cliente. Ejemplo: "SBSA".
        start_date: Fecha de inicio del rango, formato "YYYY-MM-DD".
        end_date: Fecha de fin del rango, formato "YYYY-MM-DD".
        country_df: Tabla de referencia de países (via load_country()).
        xrate_vi_df: Tipo de cambio de Visa (via load_exchange_rates()).
        vi_bin_products_df: Productos Visa por BIN.
        currency_df: Tabla de monedas numérico→alfabético.
        xrate_mc_df: Tipo de cambio de Mastercard.
        mc_bin_products_df: Productos Mastercard por BIN.
        client_cfg: Configuración del cliente (via get_client_config()).

    Returns:
        DataFrame con las 32 columnas de FINAL_COLS, o None si ninguna
        fuente tuvo datos en el rango pedido.

    Ejemplo:
        result_df = process_client_range(
            "SBSA", "2026-01-01", "2026-01-31", country_df, xrate_vi_df,
            vi_bin_products_df, currency_df, xrate_mc_df,
            mc_bin_products_df, client_cfg)
    """
    rep_cur = client_cfg["report_currency"]
    dup_vi = client_cfg["dup_on_us_visa"] and SCHEME_FEE
    dup_mc = client_cfg["dup_on_us_mc"] and SCHEME_FEE
    frames = []

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

    # TEMP: SMS deshabilitado a pedido del usuario mientras se prepara la
    # unificacion con scheme_fee -- transaction_amount/count ya validados
    # end-to-end (ver gotchas.md), pendiente solo el residual de
    # interchange_fees_amount en glue-vi-interchange. REACTIVAR cuando se
    # retome ese trabajo.
    # df_sms = read_operational(client_id, "VISA", "sms_messages", start_date, end_date)
    # if df_sms is not None:
    #     try:
    #         frames.append(
    #             transform_visa_sms(
    #                 df_sms, country_df, xrate_vi_df, vi_bin_products_df, rep_cur, dup_vi
    #             )
    #         )
    #     except Exception as e:
    #         log_error(f"[SMS] {client_id}/{start_date}_{end_date}: {e}")

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

    if SCHEME_FEE:
        cost_df = load_scheme_fee_costs(client_id, start_date, ANALYTICS_BUCKET)
        if cost_df is not None:
            result = (
                result.drop("scheme_fees_amount")
                .join(cost_df, on=["file_id", "row_id", "business_mode_code"], how="left")
                .fillna({"scheme_fees_amount": 0.0})
            )

    return result.select(FINAL_COLS)


def write_result(df: DataFrame, client_id: str) -> str:
    """
    Escribe el reporte final como un único archivo Parquet en el bucket
    S3 analytics, bajo {client_id}/reports/.

    Args:
        df: DataFrame final (32 columnas de FINAL_COLS) a persistir.
        client_id: Código de cliente, usado en el nombre del archivo.
            Ejemplo: "SBSA".

    Returns:
        Path completo de S3 donde quedó escrito el archivo.

    Ejemplo:
        s3_path = write_result(result_df, "SBSA")
        # "s3://itl-...-s3-analytics/SBSA/reports/report_transactions_SBSA_202601.parquet"
    """
    filename = f"report_transactions_{client_id}_{REPORT_SUFFIX}.parquet"

    s3_key = f"{client_id}/reports/{filename}"
    s3_path = f"s3://{ANALYTICS_BUCKET}/{s3_key}"

    log_info(f"[write_result] Writing {df.count()} rows → {s3_path}")

    df.repartition(1).write.mode("overwrite").parquet(s3_path)

    log_info(f"[write_result] Done → {s3_path}")
    return s3_path


# =============================================================================
# MAIN
# =============================================================================


def main():
    """
    Punto de entrada del job: carga las tablas de referencia una sola vez
    (cacheadas para reusar entre BASE II/SMS/Mastercard), lee la
    configuración del cliente, genera el reporte del rango pedido y lo
    escribe a S3. No recibe argumentos ni retorna nada — todos los
    parámetros vienen de los --args del job (ver CLIENT_CODE, START_DATE,
    etc. al inicio del archivo).
    """
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    log_info("=" * 70)
    log_info("glue-vi-mc-reporting Start")
    log_info(f"  client        : {CLIENT_CODE}")
    log_info(f"  range         : {START_DATE} -> {END_DATE}")
    log_info(f"  report_suffix : {REPORT_SUFFIX}")
    log_info(f"  scheme_fee    : {SCHEME_FEE}")
    log_info("=" * 70)

    country_df = load_country().cache()
    xrate_vi_df = load_exchange_rates(START_DATE, END_DATE, brand_path="Visa").cache()
    vi_bin_products_df = load_visa_bin_products().cache()
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
