"""
interchange.py — Job real: itl-0004-itx-dev-intchg-02-glue-mc-interchange
================================================================================
Archivo:     glue/scripts/mastercard/interchange/interchange.py
S3 Script:   s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/mastercard/interchange.py

Sexta etapa del pipeline Mastercard. Asigna tarifas IAR a las transacciones
de los MTIs 1240 y 1442 (los MTIs 1644/1740 no tienen lógica de
interchange, se excluyen explícitamente en main() — ver decisions.md →
"Por qué mc-interchange NO contrasta contra MTI 1644"). Lee CLN
(400_IPM_{mti}_CLN) + CAL (500_IPM_{mti}_CAL) + datos de referencia S3
(currency/, exchange-rates-glue/, mc_rules/), evalúa el motor de reglas
IAR contra cada transacción y calcula el fee resultante. Escribe a
600_IPM_{mti}_ITX/, un único Parquet consolidado por archivo TXN/CAL
(coalesce(1) + copia del part-file — ver write_single_parquet()).

Motor de reglas (assign_rules_simple): en vez de un join naive contra
todas las reglas activas, primero reduce mc_rules al universo necesario
(prefilter_rules_needed — por jurisdiction/ird/vigencia), rankea las
reglas candidatas por (region_country_code, intelica_id) con una llave
determinística (rule_key, vía row_number sobre una Window ordenada — NO
monotonically_increasing_id(), que es inestable entre shuffles, ver
gotchas.md → "glue-mc-interchange: monotonically_increasing_id() inestable
entre shuffles"), aplica un conjunto de condiciones simples (exact match,
listas separadas por coma, rangos "100-200", negación "NOT:...", más una
condición especial de monto con comparadores >=/<=/>/</=/between) y se
queda con la regla de menor rule_key por transacción
(Window.partitionBy(file_id, file_idn, ref_id) — llave de negocio estable,
no un ID sintético).

Cálculo del fee (calculate_mastercard_fee_pyspark): calculated_fee siempre
queda en la moneda de la transacción (DE_49, trx_ccy) — rate_fixed/
rate_min/rate_cap se convierten primero desde rate_currency (moneda de la
regla IAR) a trx_ccy vía el tipo de cambio de exchange-rates-glue/, y
luego fee = rate_variable * amount_trx + rate_fixed_trx, acotado entre
[rate_min_trx, rate_cap_trx].

Flujo (main → run_interchange_mti, por cada MTI transaccional en outputs):
1. Listar los Parquets CLN/CAL del file_id (filtrados por prefijo, para no
   reprocesar ejecuciones anteriores en la misma partición — ver gotcha
   "glue-mc-interchange: filtra por file_id...")
2. Por cada par TXN/CAL con el mismo stem: build_pre_eval_pyspark (join
   TXN+CAL, resolución de tipos de cambio dinámicos por moneda objetivo)
3. assign_rules_simple: asignar la regla IAR de mayor prioridad que
   matchea
4. calculate_mastercard_fee_pyspark: calcular el fee en trx_ccy
5. Escribir el resultado final a 600_IPM_{mti}_ITX/

Job Parameters:
  --JOB_NAME               nombre del Glue Job
  --S3_STAGING             s3://itl-0004-itx-dev-intchg-02-s3-staging
  --S3_REFERENCE           s3://itl-0004-itx-dev-intchg-02-s3-reference
  --client_id              ID del cliente (ej: "SBSA")
  --file_id                ID del archivo (ej: "ABC123XYZ...")
  --file_type              IN | OUT
  --file_date              YYYY-MM-DD (fecha del archivo)
  --outputs                JSON: [{"mti":"1240","s3_key":"staging/…"}, …]
  --dynamodb_table_fields  tabla DynamoDB de campos Mastercard (layout CLN)

Nota: aunque no está en la lista de argumentos requeridos, el ASL de
itl-0004-itx-dev-intchg-02-sfn-mc también pasa --content_hash a este job;
Glue lo ignora porque no está declarado en getResolvedOptions — ver
decisions.md → "Caso particular — glue-mc-interchange recibe
--content_hash pero no lo usa".
"""
import json
import os
import sys
from pyspark.context import SparkContext
 
 
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
 
import boto3
 
import uuid
import shutil
 
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
 
from pathlib import Path
from decimal import Decimal
from pyspark.sql import Window
from typing import Any, Dict, Iterable, List, Tuple
 
 
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
 
def log(msg: str) -> None:
    """
    Loguea un mensaje con el prefijo [ITX_MC_1240] a stdout (capturado por
    CloudWatch vía el logging de Glue).

    Args:
        msg: Mensaje a loguear.

    Returns:
        None.

    Ejemplo:
        log("Starting interchange job")
    """
    print(f"[ITX_MC_1240] {msg}", flush=True)


# =============================================================================
# S3 HELPERS
# =============================================================================
 
def normalize_s3(path: str) -> str:
    """
    Normaliza un URI "s3a://" a "s3://" (boto3 solo entiende "s3://";
    "s3a://" es el esquema que usa Spark/Hadoop).

    Args:
        path: URI a normalizar.

    Returns:
        URI con el esquema "s3://", o el mismo path sin cambios si no
        empezaba con "s3a://".

    Ejemplo:
        normalize_s3("s3a://bucket/key")  # "s3://bucket/key"
    """
    if path.startswith("s3a://"):
        return "s3://" + path[len("s3a://"):]
    return path
 
 
def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Separa un URI S3 en (bucket, key).

    Args:
        uri: URI S3 completo ("s3://" o "s3a://").

    Returns:
        Tupla (bucket, key).

    Raises:
        ValueError: si el URI no es un path S3 válido.

    Ejemplo:
        parse_s3_uri("s3://bucket/path/to/file.parquet")  # ("bucket", "path/to/file.parquet")
    """
    uri = normalize_s3(uri)
    if not uri.startswith("s3://"):
        raise ValueError(f"Ruta no S3: {uri}")
    rest = uri[len("s3://"):]
    bucket, key = rest.split("/", 1)
    return bucket, key
 
 
def list_s3_parquets(prefix_uri: str, region_name: str) -> List[str]:
    """
    Lista todos los archivos .parquet bajo un prefix S3, paginando
    automáticamente.

    Args:
        prefix_uri: URI del prefix a listar ("s3://" o "s3a://" — el esquema
            de los resultados respeta el del prefix recibido).
        region_name: Región AWS del bucket.

    Returns:
        Lista de URIs completos ("{scheme}://{bucket}/{key}") de los
        Parquets encontrados, ordenada alfabéticamente.

    Ejemplo:
        list_s3_parquets("s3://bucket/SBSA/MC/400_IPM_1240_CLN/file_type=IN/date=2026-02-18/", "eu-south-2")
    """
    bucket, prefix = parse_s3_uri(prefix_uri)
    s3 = boto3.client("s3", region_name=region_name)
    paginator = s3.get_paginator("list_objects_v2")
 
    out = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                scheme = "s3a" if prefix_uri.startswith("s3a://") else "s3"
                out.append(f"{scheme}://{bucket}/{key}")
    return sorted(out)
 
 
def stem_from_uri(uri: str) -> str:
    """
    Extrae el nombre de archivo sin extensión (stem) de un URI/path.

    Args:
        uri: URI o path del archivo.

    Returns:
        Stem del archivo (sin directorio ni extensión).

    Ejemplo:
        stem_from_uri("s3://bucket/.../HASH_FILEIDN25CHARS_1240.parquet")
        # "HASH_FILEIDN25CHARS_1240"
    """
    return Path(uri.rstrip("/").split("/")[-1]).stem
 
 
# =============================================================================
# DYNAMO LAYOUT / SCHEMA
# =============================================================================
 
def _clean_scalar(value: Any) -> Any:
    """
    Convierte un valor Decimal de DynamoDB a int (si es un entero exacto) o
    float. Cualquier otro tipo se devuelve sin cambios.

    Args:
        value: Valor a convertir.

    Returns:
        int, float, o el valor original sin cambios si no es Decimal.

    Ejemplo:
        _clean_scalar(Decimal('4.0'))  # 4
    """
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    return value
 
 
def load_layout_from_dynamo(table_name: str, region_name: str) -> List[Dict[str, Any]]:
    """
    Escanea completa una tabla DynamoDB de layout de campos (mastercard_fields)
    y devuelve sus items, con los valores Decimal ya normalizados a
    int/float.

    Args:
        table_name: Nombre de la tabla DynamoDB a escanear.
        region_name: Región AWS de la tabla.

    Returns:
        Lista de dicts, uno por item de la tabla (columnas como column_name,
        data_type, length, float_decimals, etc.).

    Ejemplo:
        load_layout_from_dynamo("itl-0004-itx-dev-dynamo-mastercard_fields-02", "eu-south-2")
    """
    log(f"[DYNAMO] loading layout table={table_name} region={region_name}")
 
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    table = dynamodb.Table(table_name)
 
    items: List[Dict[str, Any]] = []
    scan_kwargs: Dict[str, Any] = {}
 
    while True:
        response = table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
 
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
 
        scan_kwargs["ExclusiveStartKey"] = last_key
 
    log(f"[DYNAMO] layout rows={len(items)}")
    return [{k: _clean_scalar(v) for k, v in item.items()} for item in items]
 
 
def spark_type_from_layout(data_type: Any, length: Any = None, float_decimals: Any = None):
    """
    Traduce un data_type declarado en DynamoDB (string, int64, decimal, date,
    timestamp, etc.) al tipo Spark SQL correspondiente.

    Para decimal, deriva precisión y escala de length/float_decimals con
    límites de seguridad (precisión máxima 38, y se amplía si scale >=
    precision para evitar un DecimalType inválido).

    Args:
        data_type: Tipo declarado en DynamoDB (case-insensitive), ej.
            "decimal", "int64", "string".
        length: Longitud del campo, usada como precisión para decimal.
        float_decimals: Cantidad de decimales, usada como escala para
            decimal.

    Returns:
        Instancia de tipo Spark SQL (StringType, LongType, DoubleType,
        DecimalType, DateType, TimestampType). Cualquier data_type no
        reconocido cae a StringType.

    Ejemplo:
        spark_type_from_layout("decimal", length=10, float_decimals=4)
        # DecimalType(10, 4)
    """
    dt = str(data_type or "").strip().lower()
 
    if dt in {"string", "str", "varchar", "char"}:
        return StringType()
 
    if dt in {"int64", "bigint", "long", "integer", "int"}:
        return LongType()
 
    if dt in {"float", "double"}:
        return DoubleType()
 
    if dt == "decimal":
        precision = 38
        scale = 10
 
        try:
            raw_length = int(length)
            if raw_length > 0:
                precision = min(38, max(raw_length, 1))
        except Exception:
            pass
 
        try:
            raw_scale = int(float_decimals)
            scale = abs(raw_scale)
        except Exception:
            pass
 
        if scale >= precision:
            precision = min(38, scale + 10)
 
        return DecimalType(precision, scale)
 
    if dt == "date":
        return DateType()
 
    if dt in {"timestamp", "datetime"}:
        # para Parquet TIMESTAMP(NANOS) evitamos TimestampType en la columna problemática.
        # La columna date_and_time_local_transaction_de_12 se fuerza a LongType más abajo.
        return TimestampType()
 
    return StringType()
 
 
def build_schema_from_layout(items: Iterable[Dict[str, Any]]) -> StructType:
    """
    Construye un StructType de Spark a partir de los items de layout de
    DynamoDB, uno por column_name (duplicados de nombre se deduplican,
    gana el último visto).

    Fuerza date_and_time_local_transaction_de_12 a LongType en vez del tipo
    que indicaría su data_type declarado — evita el error "Illegal Parquet
    type: INT64 (TIMESTAMP(NANOS,false))" al leer con Spark, ya que ese
    timestamp está escrito en nanosegundos y Spark no puede inferirlo
    directamente; se lee como entero crudo y se deriva la fecha después (ver
    derive_txn_date()). También agrega file_id/ref_id/file_idn como campos
    técnicos mínimos si no vinieran ya en el layout, necesarios para los
    joins entre CLN/CAL.

    Args:
        items: Lista de items de layout (de load_layout_from_dynamo).

    Returns:
        StructType con un StructField por column_name único.

    Ejemplo:
        build_schema_from_layout(layout_items)
    """
    fields_by_name: Dict[str, StructField] = {}
 
    for item in items:
        column_name = str(item.get("column_name") or "").strip()
        if not column_name:
            continue
 
        # FIX PRINCIPAL:
        # Evita Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))
        # Spark no puede leer ese timestamp por inferencia.
        # Lo leemos como LongType y luego derivamos fecha.
        if column_name == "date_and_time_local_transaction_de_12":
            spark_type = LongType()
        else:
            spark_type = spark_type_from_layout(
                data_type=item.get("data_type"),
                length=item.get("length"),
                float_decimals=item.get("float_decimals"),
            )
 
        fields_by_name[column_name] = StructField(column_name, spark_type, True)
 
    # Campos técnicos mínimos para joins.
    for column_name, spark_type in {
        "file_id": StringType(),
        "ref_id": LongType(),
        "file_idn": StringType(),
    }.items():
        fields_by_name.setdefault(
            column_name,
            StructField(column_name, spark_type, True),
        )
 
    schema = StructType(list(fields_by_name.values()))
    log(f"[SCHEMA] TXN fields={len(schema.fields)}")
    return schema


# =============================================================================
# PARQUET READERS
# =============================================================================
 
def read_parquet(spark: SparkSession, path: str, name: str) -> DataFrame:
    """
    Lee un directorio Parquet desde S3 sin schema explícito (Spark infiere
    desde los datos). Usado para tablas de referencia (currency, mc_rules) y
    CAL.

    Args:
        spark: Sesión de Spark activa.
        path: Path S3 al directorio Parquet.
        name: Nombre descriptivo de la fuente, solo para logging.

    Returns:
        DataFrame con el contenido del directorio.

    Ejemplo:
        read_parquet(spark, f"{s3_reference}/currency/", "REF currency")
    """
    log(f"[READ] {name}: {path}")
    return spark.read.option("mergeSchema", "false").parquet(path)


def read_exchange_rates_glue(spark: SparkSession, s3_reference: str, brand: str, file_date: str) -> DataFrame:
    """
    Fuente: exchange-rates-glue/brand={brand}/exchange_date={file_date}/ (enriquecido
    con codigos numericos por glue-exchange-rates, cobertura viva y actualizada
    — reemplaza a exchange_rate/, fuente manual congelada al 2026-04-30).
    Renombra columnas a los nombres legacy (brand/currency_from/currency_to/
    currency_from_code/exchange_value) que ya espera el resto del script.
    """
    brand_partition = brand.capitalize()
    path = f"{s3_reference}/exchange-rates-glue/brand={brand_partition}/exchange_date={file_date}/"
    log(f"[READ] REF exchange-rates-glue: {path}")
    try:
        df = spark.read.option("mergeSchema", "false").parquet(path)
    except Exception as ex:
        log(f"[READ] Partition not found ({ex}). Fallback: reading full exchange-rates-glue and filtering.")
        base_path = f"{s3_reference}/exchange-rates-glue/"
        df = (
            spark.read.option("mergeSchema", "false").parquet(base_path)
            .filter(F.col("brand") == brand_partition)
            .filter(F.col("exchange_date") == file_date)
        )
    return df.select(
        F.lit(brand_partition).alias("brand"),
        F.col("from_currency").alias("currency_from"),
        F.col("to_currency").alias("currency_to"),
        F.col("from_currency_numeric_code").alias("currency_from_code"),
        F.col("to_currency_numeric_code").alias("currency_to_code"),
        F.col("fx_rate").alias("exchange_value"),
    )

 
def read_txn_with_layout(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """
    Lee el Parquet CLN de un MTI transaccional (1240/1442) con el schema
    explícito derivado del layout de DynamoDB (build_schema_from_layout), en
    vez de dejar que Spark infiera — evita el error de TIMESTAMP(NANOS) (ver
    build_schema_from_layout).

    Args:
        spark: Sesión de Spark activa.
        path: Path S3 al Parquet CLN a leer.
        schema: Schema Spark ya construido (de build_schema_from_layout).

    Returns:
        DataFrame con el schema explícito aplicado.

    Ejemplo:
        read_txn_with_layout(spark, txn_path, txn_schema)
    """
    log(f"[READ] TXN 1240 CLN with Dynamo schema: {path}")
    return (
        spark.read
        .schema(schema)
        .option("mergeSchema", "false")
        .parquet(path)
    )
 
 
def derive_txn_date(df: DataFrame, col_name: str):
    """
    Deriva una columna de fecha a partir de date_and_time_local_transaction,
    que en el schema explícito quedó como LongType (nanosegundos desde
    epoch) en vez de TimestampType — ver build_schema_from_layout().

    Args:
        df: DataFrame que contiene la columna.
        col_name: Nombre de la columna (típicamente
            "date_and_time_local_transaction").

    Returns:
        Expresión de columna DATE: si el dtype físico es bigint/long, la
        interpreta como nanosegundos epoch y convierte; si ya es un tipo
        fecha/timestamp, simplemente la castea con to_date().

    Ejemplo:
        work.withColumn("txn_date", derive_txn_date(work, "date_and_time_local_transaction"))
    """
    dtype = dict(df.dtypes).get(col_name)
 
    if dtype in {"bigint", "long"}:
        return F.to_date(
            F.from_unixtime(
                (F.col(col_name).cast("double") / F.lit(1_000_000_000)).cast("long")
            )
        )
 
    return F.to_date(F.col(col_name))
 
 
# =============================================================================
# STEP 1 - PRE_EVAL
# =============================================================================

def build_pre_eval_pyspark(
    *,
    spark: SparkSession,
    txn_path: str,
    calc_path: str,
    txn_schema: StructType,
    df_currency: DataFrame,
    df_exchange_rate: DataFrame,
    df_rules: DataFrame,
) -> DataFrame:
    """
    Construye el DataFrame base de evaluación (PRE_EVAL) para un par TXN/CAL:
    une CLN (schema explícito, ver read_txn_with_layout) con CAL por
    (file_id, ref_id, file_idn), deriva txn_date, resuelve la moneda alfa de
    la transacción por join contra currency, y normaliza todos los campos que
    usará el motor de reglas (issuer_bin_8, acquirer_bin, jurisdiction, ird,
    processing_code, etc.) con un fallback "BLANK" cuando el valor es null.

    Se lee TXN con schema Dynamo para evitar:
      Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))

    Adicionalmente, resuelve el monto de la transacción convertido a cada
    moneda de referencia que necesitan las reglas de monto (amount_transaction
    en df_rules) — por cada moneda objetivo detectada dinámicamente, agrega
    una columna amount_transaction_{ccy} con el monto ya convertido vía
    exchange-rates-glue/, para que _amount_rule_condition() pueda evaluar
    condiciones de monto en la moneda que la regla especifique.

    Args:
        spark: Sesión de Spark activa.
        txn_path: Path S3 al Parquet CLN del MTI (par actual).
        calc_path: Path S3 al Parquet CAL del MTI (par actual).
        txn_schema: Schema explícito para leer TXN (de
            build_schema_from_layout).
        df_currency: DataFrame de referencia currency (código↔alfa).
        df_exchange_rate: DataFrame de tipos de cambio (exchange-rates-glue,
            ya filtrado/normalizado por read_exchange_rates_glue).
        df_rules: DataFrame de reglas IAR (mc_rules), usado solo para
            detectar qué monedas objetivo de monto se necesitan.

    Returns:
        DataFrame con una fila por transacción, listo para
        assign_rules_simple().

    Ejemplo:
        build_pre_eval_pyspark(spark=spark, txn_path=txn_path, calc_path=calc_path,
                                txn_schema=txn_schema, df_currency=df_currency,
                                df_exchange_rate=df_exchange_rate, df_rules=df_rules)
    """
 
    txn = read_txn_with_layout(spark, txn_path, txn_schema).select(
        F.col("file_id").cast("string").alias("file_id"),
        F.col("ref_id").cast("string").alias("ref_id"),
        F.col("file_idn").cast("string").alias("file_idn"),
        F.col("pan_de_2").cast("string").alias("pan"),
        F.col("acquirer_reference_data_de_31").cast("string").alias("acquirer_reference_data"),
        F.col("processing_code_de_3").cast("string").alias("processing_code"),
        F.col("card_acceptor_business_code_[mcc]_de_26").cast("string").alias("card_acceptor_business_code"),
        F.col("date_and_time_local_transaction_de_12").alias("date_and_time_local_transaction"),
        F.col("business_activity_4_pds_158_4").cast("string").alias("ird"),
        F.col("amount_transaction_de_4").cast("string").alias("amount_transaction"),
        F.col("currency_code_transaction_de_49").cast("long").alias("currency_code_transaction"),
        F.col("mastercard_assigned_id_pds_176").cast("string").alias("mastercard_assigned_id"),
        F.col("card_present_data_de_22_6").alias("card_present_data"),
        F.col("card_acceptor_country_code_de_43_6").alias("merchant_country"),
        F.col("transaction_destination_institution_id_code_de_93").alias("transaction_destination_institution_id"),
        F.col("transaction_type_identifier_pds_43").alias("transaction_type_identifier"),
        F.col("electronic_commerce_indicator_3_pds_52_3").alias("ucaf_collection_id"),
        F.col("electronic_commerce_indicator_2_pds_52_2").alias("token_flag")                                                    
    )
 
    calc = read_parquet(spark, calc_path, "CAL 1240").select(
        F.col("file_id").cast("string").alias("file_id"),
        F.col("ref_id").cast("string").alias("ref_id"),
        F.col("file_idn").cast("string").alias("file_idn"),
        F.col("jurisdiction_assigned").cast("string").alias("jurisdiction"),
        F.col("gcms_product_identifier").cast("string").alias("gcms_product_identifier"),
        F.col("funding_source").cast("string").alias("funding_source"),
        F.col("settlement_report_amount").cast("string").alias("settlement_report_amount"),
        F.col("settlement_report_currency_code").cast("string").alias("settlement_report_currency_code"),
        F.col("card_program_identifier").alias("card_program_indicator"),
        F.col("iar_country").alias("issuer_country")
    )

    work = txn.join(calc, ["file_id", "ref_id", "file_idn"], "inner")
    work = work.withColumn("txn_date", derive_txn_date(work, "date_and_time_local_transaction"))
 
    cur = df_currency.select(
        F.col("currency_numeric_code").cast("long").alias("currency_numeric_code_num"),
        F.col("currency_alphabetic_code").cast("string").alias("trx_ccy_alpha"),
    )
 
    work = work.join(
        F.broadcast(cur),
        F.col("currency_code_transaction") == F.col("currency_numeric_code_num"),
        "left",
    )
 
    out = work.select(
        "file_id",
        "ref_id",
        "file_idn",
        F.coalesce(F.trim(F.substring(F.col("pan"), 1, 8)), F.lit("BLANK")).alias("issuer_bin_8"),
        F.coalesce(F.trim(F.substring(F.col("acquirer_reference_data"), 2, 6)), F.lit("BLANK")).alias("acquirer_bin"),
        F.coalesce(F.trim(F.col("jurisdiction")), F.lit("BLANK")).alias("jurisdiction"),
        F.coalesce(F.trim(F.col("ird")), F.lit("BLANK")).alias("ird"),
        F.coalesce(F.trim(F.substring(F.col("processing_code"), 1, 2)), F.lit("BLANK")).alias("processing_code"),
        F.coalesce(F.col("amount_transaction"), F.lit("BLANK")).alias("amount_transaction"),
        F.coalesce(F.col("settlement_report_amount"), F.lit("BLANK")).alias("settlement_report_amount"),
        F.coalesce(F.col("settlement_report_currency_code"), F.lit("BLANK")).alias("settlement_report_currency_code"),
        F.coalesce(F.col("trx_ccy_alpha"), F.lit("BLANK")).alias("amount_transaction_currency"),
        F.coalesce(F.trim(F.col("card_acceptor_business_code")), F.lit("BLANK")).alias("card_acceptor_business_code"),
        F.coalesce(F.trim(F.col("gcms_product_identifier")), F.lit("BLANK")).alias("gcms_product_identifier"),
        F.coalesce(F.trim(F.col("funding_source")), F.lit("BLANK")).alias("funding_source"),
        F.coalesce(F.col("mastercard_assigned_id"), F.lit("BLANK")).alias("mastercard_assigned_id"),

        F.coalesce(F.col("card_present_data"), F.lit("BLANK")).alias("card_present_data"),
        F.coalesce(F.col("merchant_country"), F.lit("BLANK")).alias("merchant_country"),
        F.coalesce(F.col("transaction_destination_institution_id"), F.lit("BLANK")).alias("transaction_destination_institution_id"),
        F.coalesce(F.col("transaction_type_identifier"), F.lit("BLANK")).alias("transaction_type_identifier"),
        F.coalesce(F.col("ucaf_collection_id"), F.lit("BLANK")).alias("ucaf_collection_id"),
        F.coalesce(F.col("token_flag"), F.lit("BLANK")).alias("token_flag"),

        F.coalesce(F.trim(F.col("card_program_indicator")), F.lit("BLANK")).alias("card_program_indicator"),
        F.coalesce(F.trim(F.col("issuer_country")), F.lit("BLANK")).alias("issuer_country"),
        "txn_date",
        F.col("currency_code_transaction").alias("currency_code_transaction"),
    )
 
    target_ccys = [
        r["ccy"]
        for r in (
            df_rules
            .filter(
                F.col("amount_transaction").isNotNull()
                & (F.trim(F.col("amount_transaction").cast("string")) != "")
                & F.col("amount_transaction_currency").isNotNull()
                & (F.trim(F.col("amount_transaction_currency").cast("string")) != "")
            )
            .select(F.upper(F.trim(F.col("amount_transaction_currency"))).alias("ccy"))
            .distinct()
            .collect()
        )
        if r["ccy"]
    ]
 
    log(f"[PRE_EVAL] dynamic target currencies={target_ccys}")
 
    ex = (
        df_exchange_rate
        .filter(F.upper(F.col("brand")) == F.lit("MASTERCARD"))
        .select(
            F.col("currency_from_code").cast("long").alias("currency_from_code_num"),
            F.upper(F.trim(F.col("currency_to"))).alias("currency_to_u"),
            F.col("exchange_value").cast("double").alias("exchange_value_num"),
        )
    )

    for ccy in target_ccys:
        ccy_l = ccy.lower()
        ex_ccy = ex.filter(F.col("currency_to_u") == ccy).select(
            F.col("currency_from_code_num").alias(f"currency_from_code_{ccy_l}"),
            F.col("exchange_value_num").alias(f"fx_to_{ccy_l}"),
        )
 
        out = out.join(
            F.broadcast(ex_ccy),
            F.col(f"currency_from_code_{ccy_l}") == F.col("currency_code_transaction"),
            "left",
        )
 
        out = out.withColumn(
            f"amount_transaction_{ccy_l}",
            F.coalesce(
                (
                    F.col("amount_transaction").cast("double")
                    * F.when(
                        F.upper(F.trim(F.col("amount_transaction_currency"))) == F.lit(ccy),
                        F.lit(1.0),
                    ).otherwise(F.col(f"fx_to_{ccy_l}"))
                ).cast("string"),
                F.lit("BLANK"),
            ),
        ).drop(f"currency_from_code_{ccy_l}", f"fx_to_{ccy_l}")
 
    return out


# =============================================================================
# STEP 2 - ASSIGN RULES SIMPLE
# =============================================================================


def _blank_rule_condition(rule_col: str):
    """
    Determina si una columna de regla está "en blanco" (null, o su
    representación string normalizada es "", "none", "nan" o "null") — una
    regla en blanco no restringe el matching para ese campo.

    Args:
        rule_col: Nombre de la columna de regla a evaluar (referenciada como
            columna Spark, ej. "r.processing_code").

    Returns:
        Expresión de columna booleana.

    Ejemplo:
        _blank_rule_condition("r.processing_code")
    """
    s = F.lower(F.trim(F.col(rule_col).cast("string")))
    return F.col(rule_col).isNull() | s.isin("", "none", "nan", "null")


def _norm_col(col_expr):
    """
    Normaliza una expresión de columna para comparación: recorta espacios,
    elimina espacios internos, y pasa a mayúsculas.

    Args:
        col_expr: Expresión de columna Spark a normalizar.

    Returns:
        Expresión de columna normalizada.

    Ejemplo:
        _norm_col(F.col("processing_code"))  # "00" para " 00 "
    """
    return F.upper(F.regexp_replace(F.trim(col_expr.cast("string")), " ", ""))

 
def _simple_rule_condition(rule_col: str, work_col: str):
    """
    Construye la condición de matching de un campo "simple" entre una regla y
    el valor de la transacción.

    Soporta:
      - vacío/null => no restringe
      - A
      - A,B,C
      - NOT:A,B,C
      - rangos:
            100-200
            40000000-49999999
      - mezcla:
            00,20,40-49
            NOT:100-200,300,500-600

    Args:
        rule_col: Nombre de la columna en la regla (alias "r"), ej.
            "processing_code".
        work_col: Nombre de la columna equivalente en la transacción (alias
            "w"), ej. "processing_code".

    Returns:
        Expresión de columna booleana: True si la regla está en blanco, o si
        el valor de la transacción matchea alguno de los tokens (o ninguno,
        si la regla usa NOT:).

    Ejemplo:
        _simple_rule_condition("processing_code", "processing_code")
    """
 
    raw = _norm_col(F.col(f"r.{rule_col}"))
    value = _norm_col(F.col(f"w.{work_col}"))
 
    is_blank = _blank_rule_condition(f"r.{rule_col}")
 
    is_not = raw.startswith("NOT:")
 
    clean = F.when(
        is_not,
        F.regexp_replace(raw, "^NOT:", "")
    ).otherwise(raw)
 
    tokens = F.split(clean, ",")
 
    # =========================================================
    # TOKEN MATCH
    # =========================================================
 
    def token_match(token_col):
 
        """
        Evalúa si el valor de la transacción matchea un token individual de la
        regla — por igualdad exacta, o por pertenencia a un rango numérico
        "low-high" si el token contiene un guión.

        Args:
            token_col: Expresión de columna con un token individual (ya
                explotado de la lista separada por coma).

        Returns:
            Expresión de columna booleana.
        """
        is_range = token_col.contains("-")
 
        start_val = F.split(token_col, "-").getItem(0)
        end_val = F.split(token_col, "-").getItem(1)
 
        range_match = (
            value.cast("double").between(
                start_val.cast("double"),
                end_val.cast("double")
            )
        )
 
        exact_match = (value == token_col)
 
        return F.when(
            is_range,
            range_match
        ).otherwise(
            exact_match
        )
 
    # =========================================================
    # ARRAY OF MATCHES
    # =========================================================
 
    exploded = F.explode(tokens)
 
    return (
        is_blank
        | F.when(
            is_not,
            ~F.exists(
                tokens,
                lambda x: token_match(x)
            )
        ).otherwise(
            F.exists(
                tokens,
                lambda x: token_match(x)
            )
        )
    )
 
def _amount_rule_condition(work_columns):
    """
    Construye la condición de matching para reglas de monto de transacción
    (amount_transaction), evaluada en la moneda que la propia regla
    especifica (amount_transaction_currency), usando la columna dinámica
    amount_transaction_{moneda} ya calculada en build_pre_eval_pyspark().

    Soporta amount_transaction:
      - vacío/null => no restringe
      - >=10,<=20
      - >10
      - <20
      - =15
      - between10and20

    Usa la moneda de la regla:
      amount_transaction_currency = USD
      busca columna amount_transaction_usd

    Args:
        work_columns: Nombres de columna disponibles en el DataFrame de
            transacciones (work.columns), para localizar las columnas
            amount_transaction_{moneda} dinámicas.

    Returns:
        Expresión de columna booleana.

    Ejemplo:
        _amount_rule_condition(work.columns)
    """
 
    raw_amount = F.trim(F.col("r.amount_transaction").cast("string"))
    raw_ccy = F.lower(F.trim(F.col("r.amount_transaction_currency").cast("string")))
 
    is_blank = (
        F.col("r.amount_transaction").isNull()
        | F.lower(raw_amount).isin("", "none", "nan", "null")
    )
 
    no_space = F.regexp_replace(raw_amount, " ", "")
    lower_expr = F.lower(no_space)
 
    # columna base
    amount_base = F.col("w.amount_transaction").cast("double")
 
    # usamos columna dinámica si existe:
    # amount_transaction_usd, amount_transaction_cad, etc.
    amount_value = amount_base
 
    for c in work_columns:
        if c.startswith("amount_transaction_") and c not in (
            "amount_transaction",
            "amount_transaction_currency",
        ):
            ccy = c.replace("amount_transaction_", "").lower()
            amount_value = F.when(
                raw_ccy == F.lit(ccy),
                F.col(f"w.{c}").cast("double"),
            ).otherwise(amount_value)
 
    # between10and20
    between_lo = F.regexp_extract(lower_expr, r"between(-?\d+(\.\d+)?)and(-?\d+(\.\d+)?)", 1).cast("double")
    between_hi = F.regexp_extract(lower_expr, r"between(-?\d+(\.\d+)?)and(-?\d+(\.\d+)?)", 3).cast("double")
    has_between = F.length(
        F.regexp_extract(lower_expr, r"between(-?\d+(\.\d+)?)and(-?\d+(\.\d+)?)", 1)
    ) > 0
 
    # comparadores
    ge_val = F.regexp_extract(no_space, r">=(-?\d+(\.\d+)?)", 1).cast("double")
    le_val = F.regexp_extract(no_space, r"<=(-?\d+(\.\d+)?)", 1).cast("double")
    gt_val = F.regexp_extract(no_space, r">(-?\d+(\.\d+)?)", 1).cast("double")
    lt_val = F.regexp_extract(no_space, r"<(-?\d+(\.\d+)?)", 1).cast("double")
    eq_val = F.regexp_extract(no_space, r"^=(-?\d+(\.\d+)?)$", 1).cast("double")
 
    has_ge = F.length(F.regexp_extract(no_space, r">=(-?\d+(\.\d+)?)", 1)) > 0
    has_le = F.length(F.regexp_extract(no_space, r"<=(-?\d+(\.\d+)?)", 1)) > 0
    has_gt = F.length(F.regexp_extract(no_space, r">(-?\d+(\.\d+)?)", 1)) > 0
    has_lt = F.length(F.regexp_extract(no_space, r"<(-?\d+(\.\d+)?)", 1)) > 0
    has_eq = F.length(F.regexp_extract(no_space, r"^=(-?\d+(\.\d+)?)$", 1)) > 0
 
    cond = F.lit(True)
 
    cond = cond & F.when(has_between, amount_value.between(between_lo, between_hi)).otherwise(F.lit(True))
    cond = cond & F.when(has_ge, amount_value >= ge_val).otherwise(F.lit(True))
    cond = cond & F.when(has_le, amount_value <= le_val).otherwise(F.lit(True))
    cond = cond & F.when(has_gt & ~has_ge, amount_value > gt_val).otherwise(F.lit(True))
    cond = cond & F.when(has_lt & ~has_le, amount_value < lt_val).otherwise(F.lit(True))
    cond = cond & F.when(has_eq, amount_value == eq_val).otherwise(F.lit(True))
 
    return is_blank | cond
 
def prefilter_rules_needed(df_eval: DataFrame, df_rules: DataFrame) -> DataFrame:
    """
    Reduce mc_rules al universo necesario antes del join principal, para no
    evaluar reglas irrelevantes a gran escala.

    Reduce mc_rules al universo necesario:
      - region_country_code = jurisdiction
      - ird
      - valid_from / valid_until

    Args:
        df_eval: DataFrame de transacciones (salida de
            build_pre_eval_pyspark), usado para extraer las combinaciones
            únicas de (jurisdiction, ird, txn_date) presentes.
        df_rules: DataFrame completo de reglas IAR (mc_rules).

    Returns:
        Subconjunto de df_rules cuyas (region_country_code, ird, vigencia)
        matchean al menos una combinación presente en df_eval.

    Ejemplo:
        prefilter_rules_needed(df_eval, df_rules)
    """
    work_keys = (
        df_eval
        .select(
            F.upper(F.trim(F.col("jurisdiction").cast("string"))).alias("jurisdiction_u"),
            F.upper(F.trim(F.col("ird").cast("string"))).alias("ird_u"),
            F.to_date(F.col("txn_date")).alias("txn_date_d"),
        )
        .dropDuplicates()
    )
 
    return (
        df_rules.alias("r")
        .join(
            work_keys.alias("w"),
            (
                F.upper(F.trim(F.col("r.region_country_code").cast("string"))) == F.col("w.jurisdiction_u")
            )
            & (
                F.upper(F.trim(F.col("r.ird").cast("string"))) == F.col("w.ird_u")
            )
            & (
                F.to_date(F.col("r.valid_from")) <= F.col("w.txn_date_d")
            )
            & (
                F.col("r.valid_until").isNull()
                | (F.to_date(F.col("r.valid_until")) >= F.col("w.txn_date_d"))
            ),
            "inner",
        )
        .drop("jurisdiction_u", "ird_u", "txn_date_d")
        .dropDuplicates()
    )

def assign_rules_simple(df_eval: DataFrame, df_rules: DataFrame) -> DataFrame:
    """
    Motor de reglas: asigna a cada transacción la regla IAR de mayor
    prioridad que matchea todas sus condiciones.

    Incluye:
      - prefiltro de reglas candidatas (prefilter_rules_needed)
      - join base por jurisdiction/ird/fechas
      - condiciones simples (_simple_rule_condition/_amount_rule_condition)
      - ranking legacy por region_country_code + intelica_id

    La llave de ranking (rule_key) se genera con row_number() sobre una
    Window ordenada por (region_country_code, intelica_id) — determinística y
    estable entre shuffles, a diferencia de una versión anterior que usaba
    monotonically_increasing_id() y producía asignaciones de fee incorrectas
    (count/amount correctos, solo el fee mal) porque ese ID no es estable
    entre shuffles Spark (ver gotchas.md → "glue-mc-interchange:
    monotonically_increasing_id() inestable entre shuffles"). El join final
    entre transacción y regla ganadora usa (file_id, file_idn, ref_id) —
    llave de negocio estable — en vez de un ID sintético, por el mismo
    motivo.

    Args:
        df_eval: DataFrame de transacciones (salida de
            build_pre_eval_pyspark).
        df_rules: DataFrame completo de reglas IAR (mc_rules).

    Returns:
        DataFrame con una fila por transacción, con la regla asignada
        (rule, region_country_code, intelica_id, rate_currency,
        rate_variable, rate_fixed, rate_min, rate_cap, etc.) — rule=0 y
        demás campos de regla null si ninguna regla matcheó.

    Ejemplo:
        assign_rules_simple(df_eval, df_rules)
    """
    rules_needed = prefilter_rules_needed(df_eval, df_rules)
    rules_needed_count = rules_needed.count()
    log(f"[ASSIGN_SIMPLE] rules_needed={rules_needed_count}")
 
    rules = (
        rules_needed
        .withColumn("region_country_code_u", F.upper(F.trim(F.col("region_country_code").cast("string"))))
        .withColumn("ird_u", F.upper(F.trim(F.col("ird").cast("string"))))
        .withColumn("valid_from_d", F.to_date(F.col("valid_from")))
        .withColumn("valid_until_d", F.to_date(F.col("valid_until")))
        .withColumn("_intelica_num", F.col("intelica_id").cast("long"))
        .withColumn(
            "rule_key",
            F.row_number().over(
                Window.orderBy(
                    F.col("region_country_code_u").asc_nulls_last(),
                    F.col("_intelica_num").asc_nulls_last(),
                )
            )
        )
    )
 
    work = (
        df_eval
        #.withColumn("work_id", F.monotonically_increasing_id())
        .withColumn("jurisdiction_u", F.upper(F.trim(F.col("jurisdiction").cast("string"))))
        .withColumn("ird_u", F.upper(F.trim(F.col("ird").cast("string"))))
        .withColumn("txn_date_d", F.to_date(F.col("txn_date")))
    )
 
    base = (
        work.alias("w")
        .join(
            F.broadcast(rules).alias("r"),
            (F.col("w.jurisdiction_u") == F.col("r.region_country_code_u"))
            & (F.col("w.ird_u") == F.col("r.ird_u"))
            & (F.col("r.valid_from_d") <= F.col("w.txn_date_d"))
            & (
                F.col("r.valid_until_d").isNull()
                | (F.col("r.valid_until_d") >= F.col("w.txn_date_d"))
            ),
            "left",
        )
    )
 
    simple_conditions = [
        _simple_rule_condition("processing_code", "processing_code"),
        _simple_rule_condition("card_acceptor_business_code", "card_acceptor_business_code"),
        _simple_rule_condition("gcms_product_identifier", "gcms_product_identifier"),
        _simple_rule_condition("funding_source", "funding_source"),
        _simple_rule_condition("mastercard_assigned_id", "mastercard_assigned_id"),
        
        _simple_rule_condition("card_present_data", "card_present_data"),
        _simple_rule_condition("merchant_country", "merchant_country"),
        _simple_rule_condition("transaction_destination_institution", "transaction_destination_institution_id"),
        _simple_rule_condition("tti", "transaction_type_identifier"),
        _simple_rule_condition("ucaf_collection_id", "ucaf_collection_id"),
        _simple_rule_condition("token_flag", "token_flag"),
        _simple_rule_condition("card_program_indicator", "card_program_indicator"),
        _simple_rule_condition("issuer_country", "issuer_country"),

   
        
        _amount_rule_condition(work.columns),
    ]
 
    filtered = base
    for cond in simple_conditions:
        filtered = filtered.filter(cond)
 
    ranked_rules = (
        filtered
        .filter(F.col("r.rule_key").isNotNull())
        .withColumn(
            "rn",
            F.row_number().over(
                #Window.partitionBy(F.col("w.work_id"))
                Window.partitionBy(
                    F.col("w.file_id"),
                    F.col("w.file_idn"),
                    F.col("w.ref_id")
                )
                .orderBy(F.col("r.rule_key").asc_nulls_last())
            )
        )
        .filter(F.col("rn") == 1)
        .select(
            F.col("w.file_id").alias("file_id_match"),
            F.col("w.file_idn").alias("file_idn_match"),
            F.col("w.ref_id").alias("ref_id_match"),
            #F.col("w.work_id").alias("work_id_match"),
            F.col("r.rule_key").alias("rule"),
            F.col("r.region_country_code").cast("string").alias("region_country_code"),
            F.col("r.intelica_id").cast("string").alias("intelica_id"),
            F.col("r.ird").cast("string").alias("rule_ird"),
            F.col("r.rate_currency").cast("string").alias("rate_currency"),
            F.col("r.rate_variable").cast("double").alias("rate_variable"),
            F.col("r.rate_fixed").cast("double").alias("rate_fixed"),
            F.col("r.rate_min").cast("double").alias("rate_min"),
            F.col("r.rate_cap").cast("double").alias("rate_cap"),
            F.col("r.valid_from").alias("valid_from"),
            F.col("r.valid_until").alias("valid_until"),
        )
    )

    final = (
        work.alias("w")
        .join(
            ranked_rules.alias("r"),
            #F.col("w.work_id") == F.col("r.work_id_match"),
            #"left",
            (F.col("w.file_id") == F.col("r.file_id_match"))
            & (F.col("w.file_idn") == F.col("r.file_idn_match"))
            & (F.col("w.ref_id") == F.col("r.ref_id_match")),
            "left",
        )
    )

    return final.select(
        F.col("w.file_id").alias("file_id"),
        F.col("w.ref_id").alias("ref_id"),
        F.col("w.file_idn").alias("file_idn"),

        F.coalesce(F.col("r.rule"), F.lit(0)).alias("rule"),
        F.col("r.region_country_code").alias("region_country_code"),
        F.col("r.intelica_id").alias("intelica_id"),
        F.coalesce(F.col("r.rule_ird"), F.col("w.ird").cast("string")).alias("ird"),

        F.col("r.rate_currency").alias("rate_currency"),
        F.col("r.rate_variable").alias("rate_variable"),
        F.col("r.rate_fixed").alias("rate_fixed"),
        F.col("r.rate_min").alias("rate_min"),
        F.col("r.rate_cap").alias("rate_cap"),
        F.col("r.valid_from").alias("valid_from"),
        F.col("r.valid_until").alias("valid_until"),

        F.col("w.jurisdiction").alias("jurisdiction"),
        F.col("w.processing_code").alias("processing_code"),
        F.col("w.card_acceptor_business_code").alias("card_acceptor_business_code"),
        F.col("w.amount_transaction").alias("amount_transaction"),
        F.col("w.amount_transaction_currency").alias("amount_transaction_currency"),
        F.col("w.settlement_report_amount").alias("settlement_report_amount"),
        F.col("w.settlement_report_currency_code").alias("settlement_report_currency_code"),
        F.col("w.txn_date").alias("txn_date"),
        F.col("w.currency_code_transaction").alias("currency_code_transaction"),
        F.col("w.issuer_bin_8").alias("issuer_bin_8"),
        F.col("w.acquirer_bin").alias("acquirer_bin"),
        F.col("w.gcms_product_identifier").alias("gcms_product_identifier"),
        F.col("w.funding_source").alias("funding_source"),
        F.col("w.mastercard_assigned_id").alias("mastercard_assigned_id"),
        
        F.col("w.card_present_data").alias("card_present_data"),
        F.col("w.merchant_country").alias("merchant_country"),
        F.col("w.transaction_destination_institution_id").alias("transaction_destination_institution_id"),
        F.col("w.transaction_type_identifier").alias("transaction_type_identifier"),
        F.col("w.ucaf_collection_id").alias("ucaf_collection_id"),
        F.col("w.token_flag").alias("token_flag"),
        F.col("w.card_program_indicator").alias("card_program_indicator"),
        F.col("w.issuer_country").alias("issuer_country"),
    )       

def calculate_mastercard_fee_pyspark(
    df_assign: DataFrame,
    df_exchange_rate: DataFrame,
    brand_fx_eval: str = "MASTERCARD",
) -> DataFrame:
    
    """
    Calcula el Interchange Fee Mastercard.

    Conceptos:

    - amount_transaction:
        Monto original de la transacción.
    - transaction currency:
        Moneda original de la transacción.
    - rate currency:
        Moneda en la que la regla Mastercard está definida.
    - settlement currency:
        Moneda final utilizada para liquidar el fee.
    - FX (Foreign Exchange):
        Tipo de cambio utilizado para convertir importes entre monedas.

    Proceso:

    1. Obtener fx(rate_ccy -> trx_ccy). Si rate_ccy es NONE/vacio o igual
       a trx_ccy, fx_to_trx = 1.0 (sin conversion).
    2. Escalar rate_fixed, rate_min, rate_cap a trx_ccy multiplicando por fx_to_trx.
    3. Calcular fee en trx_ccy:
       fee = rate_variable * amount_trx + rate_fixed * fx_to_trx
    4. Aplicar restricciones (en trx_ccy):
       fee_final = min(rate_cap_trx, max(rate_min_trx, fee))

    Args:
        df_assign: DataFrame de transacciones con regla ya asignada (salida
            de assign_rules_simple).
        df_exchange_rate: DataFrame de tipos de cambio (exchange-rates-glue).
        brand_fx_eval: Marca a filtrar en df_exchange_rate (default:
            "MASTERCARD").

    Returns:
        df_assign con 3 columnas nuevas agregadas:
        - calculated_fee: Fee en moneda de transaccion (DE_49), listo para
          escribir al ITX.
        - fx_to_trx: Tipo de cambio utilizado: rate_ccy -> trx_ccy.
        - fee_preliminary: Fee antes de aplicar rate_min/rate_cap (para
          debugging).

    Ejemplo:
        calculate_mastercard_fee_pyspark(df_assign, df_exchange_rate)
    """

    # ============================================================================
    # STEP 1
    # Normalización de datos y conversión de tipos
    # ============================================================================
    
    a = (
        df_assign
        .withColumn("amount_transaction_num", F.col("amount_transaction").cast("double"))
        .withColumn("rate_variable_num", F.col("rate_variable").cast("double"))
        .withColumn("rate_fixed_num", F.col("rate_fixed").cast("double"))
        .withColumn("rate_min_num", F.col("rate_min").cast("double"))
        .withColumn("rate_cap_num", F.col("rate_cap").cast("double"))
        .withColumn("txn_date_d", F.to_date("txn_date"))
        .withColumn("rate_currency_u", F.upper(F.trim(F.col("rate_currency").cast("string"))))
        .withColumn("trx_currency_u", F.upper(F.trim(F.col("amount_transaction_currency").cast("string"))))
        .withColumn("settlement_currency_u", F.upper(F.trim(F.col("settlement_report_currency_code").cast("string"))))
    )
    
    # ============================================================================
    # STEP 2
    # Cargar tipos de cambio Mastercard: rate_ccy -> trx_ccy
    # ============================================================================
    ex_settle = (
        df_exchange_rate
        .filter(F.upper(F.col("brand")) == F.upper(F.lit(brand_fx_eval)))
        .select(
            F.upper(F.trim(F.col("currency_from").cast("string"))).alias("currency_from_u"),
            F.upper(F.trim(F.col("currency_to").cast("string"))).alias("currency_to_u"),
            F.col("exchange_value").cast("double").alias("exchange_value_num"),
        )
        .dropDuplicates(["currency_from_u", "currency_to_u"])
        .alias("ex_settle")
    )

    # ============================================================================
    # STEP 3
    # Join para obtener fx(rate_ccy -> trx_ccy).
    # Convierte rate_fixed, rate_min, rate_cap a la moneda de transacción (DE_49).
    # ============================================================================
    joined = (
        a.alias("a")
        .join(
            F.broadcast(ex_settle),
            (F.col("ex_settle.currency_from_u") == F.col("a.rate_currency_u"))
            & (F.col("ex_settle.currency_to_u") == F.col("a.trx_currency_u")),
            "left",
        )
    )

    # ============================================================================
    # STEP 4
    # FX rate_ccy -> trx_ccy.
    # 1.0 cuando rate_ccy es NONE, vacío o igual a trx_ccy.
    # ============================================================================
    fx_to_trx = (
        F.when(
            F.col("a.rate_currency_u").isNull()
            | (F.col("a.rate_currency_u") == "")
            | (F.col("a.rate_currency_u") == F.col("a.trx_currency_u")),
            F.lit(1.0),
        )
        .otherwise(F.col("ex_settle.exchange_value_num"))
    )

    # ============================================================================
    # STEP 5
    # Convertir componentes fijos a trx_ccy y calcular fee preliminar.
    #
    # fee = rate_variable * amount_trx + rate_fixed * fx(rate_ccy -> trx_ccy)
    #
    # rate_fixed, rate_min, rate_cap se escalan con fx_to_trx para quedar
    # en la misma moneda que el monto de la transacción.
    # ============================================================================
    rate_fixed_trx = F.when(
        F.col("a.rate_fixed_num").isNull(), F.lit(0.0)
    ).otherwise(F.col("a.rate_fixed_num") * fx_to_trx)

    rate_min_trx = F.when(
        F.col("a.rate_min_num").isNull(), F.lit(-1e18)
    ).otherwise(F.col("a.rate_min_num") * fx_to_trx)

    rate_cap_trx = F.when(
        F.col("a.rate_cap_num").isNull(), F.lit(1e18)
    ).otherwise(F.col("a.rate_cap_num") * fx_to_trx)

    fee_preliminary = (
        F.coalesce(F.col("a.rate_variable_num"), F.lit(0.0)) * F.col("a.amount_transaction_num")
        + rate_fixed_trx
    )

    # ============================================================================
    # STEP 6
    # Aplicar restricciones de la regla (todas en trx_ccy).
    #
    # fee_final = min(rate_cap_trx, max(rate_min_trx, fee))
    # ============================================================================
    calculated_fee = (
        F.when(
            F.col("a.rate_variable").isNull(),
            rate_fixed_trx,
        )
        .when(F.col("a.rate_variable_num").isNull(), F.lit(None).cast("double"))
        .when(F.col("a.amount_transaction_num").isNull(), F.lit(None).cast("double"))
        .when(
            (F.col("a.rate_currency_u").isNotNull())
            & (F.col("a.rate_currency_u") != "")
            & (F.col("a.rate_currency_u") != F.col("a.trx_currency_u"))
            & F.col("ex_settle.exchange_value_num").isNull(),
            F.lit(None).cast("double"),
        )
        .otherwise(
            F.least(
                rate_cap_trx,
                F.greatest(
                    rate_min_trx,
                    fee_preliminary,
                ),
            )
        )
    )

    # ============================================================================
    # STEP 7
    # Construcción del resultado final
    # ============================================================================
    base_cols = [F.col(f"a.{c}").alias(c) for c in df_assign.columns]

    return (
        joined
        .withColumn("fx_to_trx", fx_to_trx)
        .withColumn("fee_preliminary", fee_preliminary)
        .withColumn("calculated_fee", calculated_fee)
        .select(
            *base_cols,
            "fx_to_trx",
            "fee_preliminary",
            "calculated_fee",
        )
    )
    
# =============================================================================
# WRITE HELPERS
# =============================================================================
 
def is_s3_path(path: str) -> bool:
    """
    Detecta si un path es una ruta S3 ("s3://" o "s3a://").

    Args:
        path: Path a evaluar.

    Returns:
        True si empieza con "s3://" o "s3a://".

    Ejemplo:
        is_s3_path("s3a://bucket/key")  # True
    """
    return path.startswith("s3://") or path.startswith("s3a://")
 
def write_single_parquet(df: DataFrame, final_file_path: str, region_name: str) -> None:
    """
    Escribe un único archivo .parquet por cada par TXN/CAL, en vez de la
    carpeta con part-*.parquet que Spark produce por defecto.

    Nota:
      Spark siempre escribe carpetas con part-*.parquet.
      Por eso escribimos en una carpeta temporal y luego movemos/copiamos el part.

    Para S3: escribe a un prefix temporal único (coalesce(1) + write), copia
    el único part-file al key final con copy_object, y borra el prefix
    temporal completo. Para filesystem local: mismo patrón con shutil.move.

    Args:
        df: DataFrame a escribir (se fuerza a 1 solo part-file con
            coalesce(1)).
        final_file_path: Path final deseado (S3 o local), incluyendo el
            nombre de archivo ".parquet".
        region_name: Región AWS del bucket (solo relevante si es S3).

    Returns:
        None.

    Raises:
        RuntimeError: si no se encuentra ningún part-file tras escribir la
            carpeta temporal.

    Ejemplo:
        write_single_parquet(df_fee_final, final_file, aws_region)
    """
    tmp_suffix = f"_tmp_{uuid.uuid4().hex}"
 
    if is_s3_path(final_file_path):
        s3 = boto3.client("s3", region_name=region_name)
        final_bucket, final_key = parse_s3_uri(final_file_path)
 
        if final_key.endswith(".parquet"):
            final_key_base = final_key[:-8]
        else:
            final_key_base = final_key
 
        tmp_prefix = f"{final_key_base}_{tmp_suffix}/"
        tmp_uri = f"s3a://{final_bucket}/{tmp_prefix}"
 
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .parquet(tmp_uri)
        )
 
        response = s3.list_objects_v2(Bucket=final_bucket, Prefix=tmp_prefix)
        part_keys = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".parquet") and "/part-" in obj["Key"]
        ]
 
        if not part_keys:
            raise RuntimeError(f"No se encontró part parquet en {tmp_uri}")
 
        part_key = part_keys[0]
 
        s3.copy_object(
            Bucket=final_bucket,
            CopySource={"Bucket": final_bucket, "Key": part_key},
            Key=final_key,
        )
 
        delete_objects = [{"Key": obj["Key"]} for obj in response.get("Contents", [])]
        if delete_objects:
            s3.delete_objects(
                Bucket=final_bucket,
                Delete={"Objects": delete_objects},
            )
 
        log(f"[WRITE] OK -> s3://{final_bucket}/{final_key}")
        return
 
    final_path = Path(final_file_path)
    tmp_dir = final_path.parent / tmp_suffix
 
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(tmp_dir))
    )
 
    part_files = list(tmp_dir.glob("part-*.parquet"))
    if not part_files:
        raise RuntimeError(f"No se encontró part parquet en {tmp_dir}")
 
    final_path.parent.mkdir(parents=True, exist_ok=True)
 
    if final_path.exists():
        final_path.unlink()
 
    shutil.move(str(part_files[0]), str(final_path))
    shutil.rmtree(tmp_dir, ignore_errors=True)
 
    log(f"[WRITE] OK -> {final_path}")
 
def build_output_file_path(
    *,
    output_base: str,
    client_id: str,
    file_type: str,
    process_date: str,
    source_file_name: str,
    target_subdir: str = "600_IPM_1240_ITX_PRE_EVAL",
) -> str:
    """
    Construye el path de salida para un archivo de una etapa intermedia o
    final del job.

    Ruta temporal para validar PRE_EVAL.

    Local:
      /.../output/SBSA/MC/600_IPM_1240_ITX_PRE_EVAL/file_type=IN/date=YYYY-MM-DD/file.parquet

    S3:
      s3a://bucket/.../SBSA/MC/600_IPM_1240_ITX_PRE_EVAL/file_type=IN/date=YYYY-MM-DD/file.parquet

    Args:
        output_base: Prefix base de salida (S3 o local).
        client_id: Código de cliente.
        file_type: "IN" u "OUT".
        process_date: Fecha del archivo, formato "YYYY-MM-DD".
        source_file_name: Stem del archivo TXN/CAL (identifica el par
            procesado).
        target_subdir: Subdirectorio de destino (default:
            "600_IPM_1240_ITX_PRE_EVAL"; en el flujo real se pasa
            "600_IPM_{mti}_ITX" u otro subdir de debug).

    Returns:
        Path completo de salida, incluyendo el nombre de archivo ".parquet".

    Ejemplo:
        build_output_file_path(output_base=s3_staging, client_id="SBSA",
                                file_type="IN", process_date="2026-02-18",
                                source_file_name="HASH_FILEIDN_1240",
                                target_subdir="600_IPM_1240_ITX")
    """
    return (
        f"{output_base.rstrip('/')}/{client_id}/MC/{target_subdir}"
        f"/file_type={file_type}/date={process_date}/{source_file_name}.parquet"
    )

# =============================================================================
# RUNNER
# =============================================================================

def run_interchange_mti(
    *,
    spark: SparkSession,
    s3_staging: str,
    s3_reference: str,
    client_id: str,
    file_id: str,
    file_type: str,
    file_date: str,
    layout_table: str,
    aws_region: str,
    mti: str,
) -> None:
    """
    Ejecuta el pipeline de interchange completo para un MTI de un archivo:
    lee TXN/CAL, construye PRE_EVAL, asigna reglas, calcula el fee y escribe
    el resultado final.

    Glue runner final:
      - Lee TXN 1240/1440 CLN
      - Lee CAL 1240/1440
      - Construye PRE_EVAL
      - Asigna reglas
      - Calcula fee
      - Escribe SOLO el parquet final en:
        {s3_staging}/{client_id}/MC/600_IPM_{mti}_ITX/file_type={file_type}/date={file_date}/<source>.parquet

    Solo procesa los archivos cuyo nombre comienza con file_id, evitando
    reprocesar parquets de otras ejecuciones anteriores en la misma partición
    de fecha (file_type=X/date=YYYY-MM-DD).

    Args:
        spark: Sesión de Spark activa.
        s3_staging: Bucket/prefix base de staging.
        s3_reference: Bucket/prefix base de reference.
        client_id: Código de cliente.
        file_id: Identificador del archivo a procesar (filtro de prefijo de
            nombre de archivo).
        file_type: "IN" u "OUT".
        file_date: Fecha del archivo, formato "YYYY-MM-DD".
        layout_table: Tabla DynamoDB de layout de campos Mastercard.
        aws_region: Región AWS.
        mti: MTI a procesar ("1240" o "1442").

    Returns:
        None — escribe el/los Parquet(s) finales directamente a S3. No hace
        nada (log + return) si no hay archivos TXN o CAL para ese file_id.

    Ejemplo:
        run_interchange_mti(spark=spark, s3_staging=s3_staging, s3_reference=s3_reference,
                             client_id="SBSA", file_id="ABC123...", file_type="IN",
                             file_date="2026-02-18", layout_table=dynamo_table_fields,
                             aws_region="eu-south-2", mti="1240")
    """
 
    txn_prefix =  (f"{s3_staging}/{client_id}/MC/400_IPM_{mti}_CLN/"f"file_type={file_type}/date={file_date}/")
    calc_prefix = (f"{s3_staging}/{client_id}/MC/500_IPM_{mti}_CAL/"f"file_type={file_type}/date={file_date}/")
 
    currency_path = f"{s3_reference}/currency/"
    rules_path = f"{s3_reference}/mc_rules/"
 
    # Salida final en STAGING, no en carpeta local.
    #output_base = s3_staging.rstrip("/")
    output_base = os.getenv("OUTPUT_BASE",s3_staging.rstrip()).rstrip("/")
    
    target_subdir = f"600_IPM_{mti}_ITX"
 
    # Para Glue para pruebas
    #   - deja MAX_PAIRS alto para procesar todo
    #   - si quieres probar 1 archivo, setea MAX_PAIRS=1 como env var o cambia el default temporalmente
    max_pairs = int(os.getenv("MAX_PAIRS", "999999"))

    # FIX: filtrar por file_id para procesar únicamente los archivos
    # correspondientes a la ejecución actual. Sin este filtro, el job listaba
    # TODOS los parquets de la partición (date=YYYY-MM-DD) y reprocesaba
    # archivos de ejecuciones anteriores, actualizando su Last-Modified
    # innecesariamente.
    file_id_upper = file_id.upper()
 
    all_txn_files = list_s3_parquets(txn_prefix, aws_region)
    txn_files = [p for p in all_txn_files if stem_from_uri(p).upper().startswith(file_id_upper)]
 
    log(f"[FILTER] MTI={mti} TXN total_in_prefix={len(all_txn_files)} matched_file_id={len(txn_files)}")
 
    if not txn_files:
        log(f"[SKIP] MTI={mti} no TXN files found for file_id={file_id} path={txn_prefix}")
        return
 
    all_calc_files = list_s3_parquets(calc_prefix, aws_region)
    calc_files = [p for p in all_calc_files if stem_from_uri(p).upper().startswith(file_id_upper)]
 
    log(f"[FILTER] MTI={mti} CAL total_in_prefix={len(all_calc_files)} matched_file_id={len(calc_files)}")
 
    if not calc_files:
        log(f"[SKIP] MTI={mti} no CAL files found for file_id={file_id} path={calc_prefix}")
        return
    
    log(f"[JOB] Mastercard {mti} ITX")
    log(f"[JOB] TXN={len(txn_files)} CAL={len(calc_files)}")
    log(f"[JOB] output={output_base}/{client_id}/MC/{target_subdir}/file_type={file_type}/date={file_date}/")
 
    layout_items = load_layout_from_dynamo(layout_table, aws_region)
    txn_schema = build_schema_from_layout(layout_items)
 
    df_currency = read_parquet(spark, currency_path, "REF currency").cache()
    df_exchange_rate = read_exchange_rates_glue(spark, s3_reference, "MasterCard", file_date).cache()
    df_rules = read_parquet(spark, rules_path, "REF mc_rules").cache()

    calc_by_key: Dict[str, str] = {
        stem_from_uri(p): p
        for p in calc_files
    }
 
    processed = 0
    skipped = 0
    failed = 0
 
    for txn_path in txn_files[:max_pairs]:
        key = stem_from_uri(txn_path)
        calc_path = calc_by_key.get(key)
 
        if not calc_path:
            log(f"[WARN] No CAL match for TXN={txn_path}")
            skipped += 1
            continue
 
        try:
            log(f"[PAIR] START MTI={mti} key={key}")
 
            df_eval = build_pre_eval_pyspark(
                spark=spark,
                txn_path=txn_path,
                calc_path=calc_path,
                txn_schema=txn_schema,
                df_currency=df_currency,
                df_exchange_rate=df_exchange_rate,
                df_rules=df_rules,
            )

            if os.getenv("DEBUG_WRITE_STEPS", "0") == "1":
                pre_eval_file = build_output_file_path(
                    output_base=output_base,
                    client_id=client_id,
                    file_type=file_type,
                    process_date=file_date,
                    source_file_name=key,
                    target_subdir=f"DEBUG_{mti}_PRE_EVAL",
                )
                write_single_parquet(df_eval, pre_eval_file, aws_region)

            df_assign = assign_rules_simple(
                df_eval=df_eval,
                df_rules=df_rules,
            )

            if os.getenv("DEBUG_WRITE_STEPS", "0") == "1":
                assign_file = build_output_file_path(
                    output_base=output_base,
                    client_id=client_id,
                    file_type=file_type,
                    process_date=file_date,
                    source_file_name=key,
                    target_subdir=f"DEBUG_{mti}_ASSIGN",
                )
                write_single_parquet(df_assign, assign_file, aws_region)
 
            df_fee = calculate_mastercard_fee_pyspark(
                df_assign=df_assign,
                df_exchange_rate=df_exchange_rate,
                brand_fx_eval="MASTERCARD",
            )

            df_fee_final = df_fee.select(
                F.col("file_id"),
                F.col("file_idn"),
                F.col("ref_id"),
                F.col("rate_currency"),
                F.col("rate_variable"),
                F.col("rate_fixed"),
                F.col("rate_min"),
                F.col("rate_cap"),
                F.col("amount_transaction"),
                F.col("currency_code_transaction").cast("string").alias("currency_transaction"),
                F.col("intelica_id"),
                F.col("calculated_fee").alias("calculated_value"),
                F.col("region_country_code"),
                F.col("ird"),
                F.col("valid_from"),
                F.col("valid_until"),
            )
 
            final_file = build_output_file_path(
                output_base=output_base,
                client_id=client_id,
                file_type=file_type,
                process_date=file_date,
                source_file_name=key,
                target_subdir=target_subdir,
            )
 
            write_single_parquet(df_fee_final, final_file, aws_region)
 
            processed += 1
            log(f"[PAIR] OK MTI={mti} key={key} output={final_file}")
 
        except Exception as exc:
            failed += 1
            log(f"[PAIR] ERROR MTI={mti} key={key} error={exc}")
            continue
 
    log(f"[SUMMARY] MTI={mti} processed={processed} skipped={skipped} failed={failed}")


# =============================================================================
# MAIN — estandarizado con mc_calculate.py y vi_calculate.py
# =============================================================================
# Job Parameters (pasados por el orquestador en cada ejecución):
#   --JOB_NAME              nombre del Glue Job
#   --S3_STAGING            s3://itl-0004-itx-dev-intchg-02-s3-staging
#   --S3_REFERENCE          s3://itl-0004-itx-dev-intchg-02-s3-reference
#   --client_id             ID del cliente  (ej: "CLIENT01")
#   --file_id               ID del archivo  (ej: "ABC123XYZ...")
#   --file_type             IN | OUT
#   --file_date             YYYY-MM-DD  (fecha del archivo)
#   --outputs               JSON: [{"mti":"1240","s3_key":"staging/…"}, …]
#   --dynamodb_table_fields tabla DynamoDB de campos Mastercard
# =============================================================================

def main() -> None:
    """
    Punto de entrada del Glue job glue-mc-interchange. Resuelve los
    argumentos del job, inicializa la sesión Spark/Glue con la configuración
    de compatibilidad Parquet necesaria (timestamps en microsegundos,
    nanosAsLong, vectorized reader deshabilitado), deriva la lista de MTIs
    transaccionales a procesar desde --outputs (excluyendo 1644/1740, que no
    tienen lógica de interchange) y ejecuta run_interchange_mti() para cada
    uno.

    Returns:
        None. Llama a job.commit() y spark.stop() al finalizar.

    Ejemplo:
        main()  # invocado automáticamente al ejecutar el script como Glue job
    """
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_STAGING",
            "S3_REFERENCE",
            "client_id",
            "file_id",
            "file_type",
            "file_date",
            "outputs",
            "dynamodb_table_fields",
        ],
    )
 
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
 
    spark.sparkContext.setLogLevel("ERROR")
 
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
 
    # Parquet compatibility
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
 
    # Performance local/Glue
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.broadcastTimeout", "600")
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "32"))
 
    # Parámetros principales
    client_id           = args["client_id"]
    file_id             = args["file_id"]
    file_type           = args["file_type"]
    file_date           = args["file_date"]           # YYYY-MM-DD
    s3_staging          = args["S3_STAGING"].rstrip("/")
    s3_reference        = args["S3_REFERENCE"].rstrip("/")
    dynamo_table_fields = args["dynamodb_table_fields"]
    outputs             = json.loads(args["outputs"])
    aws_region          = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "eu-south-2"))
 
    log("=" * 70)
    log("MC-INTERCHANGE (PySpark/Glue 4.0) — STARTING")
    log("=" * 70)
    log(f"  JOB_NAME:              {args['JOB_NAME']}")
    log(f"  client_id:             {client_id}")
    log(f"  file_id:               {file_id}")
    log(f"  file_type:             {file_type}")
    log(f"  file_date:             {file_date}")
    log(f"  S3_STAGING:            {s3_staging}")
    log(f"  S3_REFERENCE:          {s3_reference}")
    log(f"  dynamodb_table_fields: {dynamo_table_fields}")
    log(f"  Total outputs:         {len(outputs)}")
    log("=" * 70)
 
    # Derivar lista de MTIs desde el array de outputs (reemplaza MTI_LIST env var).
    # Se excluyen MTIs no transaccionales (1644, 1740) que no tienen lógica en interchange.
    _INTERCHANGE_SKIP_MTIS = {"1644", "1740"}
    mti_list = list(dict.fromkeys(
        str(o.get("mti", ""))
        for o in outputs
        if str(o.get("mti", "")) not in _INTERCHANGE_SKIP_MTIS
        and str(o.get("mti", "")).strip()
    ))
 
    if not mti_list:
        log("[WARN] No MTIs transaccionales encontrados en outputs. Abortando.")
    else:
        log(f"  MTIs a procesar: {mti_list}")
 
    for mti in mti_list:
        run_interchange_mti(
            spark=spark,
            s3_staging=s3_staging,
            s3_reference=s3_reference,
            client_id=client_id,
            file_id=file_id,
            file_type=file_type,
            file_date=file_date,
            layout_table=dynamo_table_fields,
            aws_region=aws_region,
            mti=mti,
        )
 
    job.commit()
    spark.stop()
 
 
if __name__ == "__main__":
    main()