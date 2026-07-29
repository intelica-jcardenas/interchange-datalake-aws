# =============================================================================
# ITX-CALCULATE (PySpark) - AWS Glue Job
# =============================================================================
# Calcula campos adicionales a partir de datos limpios (Clean)
# Soporta: BASEII (drafts), SMS (messages), VSS (settlement 110/120/130/140)
# =============================================================================
 
"""
calculate.py — Job real: itl-0004-itx-dev-intchg-02-glue-vi-calculate
================================================================================
Archivo:     glue/scripts/visa/calculate/calculate.py
S3 Script:   s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py

Cuarta etapa del pipeline Visa. Calcula los campos derivados necesarios
para la tarificación de interchange a partir del CLN (clean): tipo de
transacción, ciclo, país/región del emisor (cruzando con el ARDEF vigente
a la fecha del archivo), jurisdiction (on-us/off-us/intraregional/
interregional), moneda de settlement, timeliness, entre otros. Soporta
los 3 tipos de record del pipeline Visa: BASEII (drafts, 30 campos), SMS
(messages, 27 campos) y VSS (settlement 110/120/130/140, 2 campos). Lee
CLN desde s3-staging/300_*/, escribe CAL a s3-staging/400_*/. Se usa Glue
(PySpark) y no Lambda por la complejidad de las lógicas y el volumen de
datos — ver decisions.md → "Por qué Glue y no Lambda para Calculate e
Interchange".

El ARDEF (rangos de BINes y reglas Visa) se carga y filtra 100% en Spark
(sin toPandas(), para soportar ejecución concurrente sin colapsar la
memoria del driver — ver decisions.md → "Por qué el diseño es
configuration-driven"). El join contra ARDEF (join_with_ardef) usa
bucketing por prefijo de 3 dígitos del número de cuenta para convertir un
range-join costoso en un broadcast hash join exacto por prefijo + rango,
reduciendo el tiempo de ejecución de minutos a segundos.

BASEII y SMS comparten la mayoría de la lógica de negocio (jurisdiction,
timeliness, joins ARDEF/country/currency) con variantes de columna fuente
por marca de record (ej. account_number vs card_number) — cada campo
calculado tiene una función `_draft`/`_sms` separada cuando la lógica
difiere.

Flujo (por cada output del archivo, vía process_output):
1. Cargar el CLN del output (BASEII/SMS/VSS_xxx)
2. BASEII/SMS: join con ARDEF, calcular campos directos, campos con
   coalesce/string manipulation, lógica condicional (business_mode,
   business_transaction_type/cycle, reversal_indicator), joins con
   country/currency, jurisdiction + jurisdiction_assigned +
   settlement_report_currency_code, timeliness
   VSS: vss_report_type + vss_aggregation_level (jerarquía de rollup)
3. Escribir el CAL resultante a s3-staging/400_*/

Job Parameters:
  --JOB_NAME               nombre del Glue Job
  --reference_bucket       bucket s3-reference
  --staging_bucket         bucket s3-staging
  --client_id              ID del cliente, ej. "EBGR"
  --file_id                ID del archivo
  --file_type              IN | OUT
  --file_date              YYYY-MM-DD (fecha del archivo, usada para filtrar el ARDEF vigente)
  --outputs                JSON: [{"output_type": "BASEII", "s3_key": "staging/…"}, …]
  --dynamodb_table_client  tabla DynamoDB de clientes (local_currency_code, BINs, etc.)
"""
import sys
import json
from datetime import datetime, date
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, DateType
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
 
 
def log_info(message: str):
    logger.info(f"GlueLogger: {message}")
 
 
def log_error(message: str):
    logger.error(f"GlueLogger: {message}")


# =============================================================================
# HELPERS: CARGA DE DATOS DE REFERENCIA
# =============================================================================

def load_reference_table(bucket: str, table_name: str) -> DataFrame:
    """
    Carga una tabla de referencia desde S3.

    Args:
        bucket: Bucket S3 de referencia.
        table_name: Nombre de la tabla (subdirectorio bajo el bucket), ej.
            "country", "currency".

    Returns:
        DataFrame con el contenido de {table_name}/data.parquet.

    Ejemplo:
        load_reference_table(reference_bucket, "country")
    """
    path = f"s3://{bucket}/{table_name}/data.parquet"
    log_info(f"Loading reference table: {path}")
    return spark.read.parquet(path)
 
 
def load_visa_ardef(reference_bucket: str, file_date: date) -> DataFrame:
    """
    Carga y prepara el ARDEF de Visa (rangos de BINes y reglas), filtrado a
    los rangos vigentes para la fecha del archivo. Procesado 100% en Spark —
    sin toPandas() para soportar ejecución concurrente sin colapso de memoria
    en el driver.

    Pasos (ver comentarios inline numerados en el código para el detalle de
    cada uno): filtrar por delete_indicator, convertir effective_date/
    valid_until a DateType, filtrar por vigencia contra file_date, castear
    las llaves de rango a numérico, deduplicar por low_key_for_range
    (quedándose con la versión más reciente — replica exacto la CTE
    `ardef_pre_r` de legacy, ver `adapters.py`), seleccionar los campos
    necesarios, renombrar product_id→ardef_product_id (evita colisión en
    joins) y cachear el resultado (se usa múltiples veces en
    calculate_baseii_fields/calculate_sms_fields).

    A diferencia de versiones anteriores, este DataFrame NO garantiza rangos
    sin solapamiento entre sí — igual que legacy, distintos low_key_for_range
    pueden seguir cubriendo la misma cuenta (ej. un rango viejo y ancho junto
    a uno nuevo y angosto anidado dentro). La resolución de qué rango gana
    para cada transacción ocurre en `join_with_ardef()`, después del join,
    usando el mismo criterio que legacy (`effective_date` más reciente,
    empate por rango más ancho) — ver ese docstring y la decisión "Por qué
    join_with_ardef() resuelve solapamientos por transacción" en
    `decisions.md`.

    Args:
        reference_bucket: Bucket S3 de referencia.
        file_date: Fecha del archivo, usada para filtrar los rangos ARDEF
            vigentes a esa fecha.

    Returns:
        DataFrame ARDEF filtrado, deduplicado y cacheado.

    Ejemplo:
        load_visa_ardef(reference_bucket, date(2026, 1, 3))
    """
    path = f"s3://{reference_bucket}/visa_ardef/data.parquet"
    log_info(f"Loading ARDEF from: {path}")
 
    file_date_str = file_date.strftime("%Y-%m-%d") if isinstance(file_date, date) else str(file_date)
 
    # ── 1. Lectura y filtrado inicial en Spark ────────────────────────────────
    # Nota: el pre-filtro por fecha se removió de aquí porque effective_date
    # viene en formato 'yyyyMMdd' (sin separadores) y valid_until en 'yyyy-MM-dd'
    # — comparar esos strings contra file_date_str ('yyyy-MM-dd') es comparar
    # formatos distintos (lexicográficamente incorrecto). El filtro real de
    # fechas se aplica en el paso 3, después de convertir ambas a DateType.
    ardef = spark.read.parquet(path).filter(F.col("delete_indicator") == " ")

    # ── 2. Convertir fechas y rellenar valid_until nulo con file_date ─────────
    # effective_date viene como 'yyyyMMdd' (ej. '20131018') — requiere formato
    # explícito porque to_date() sin formato espera ISO 'yyyy-MM-dd' y devuelve
    # NULL para 'yyyyMMdd', vaciando el ARDEF completo tras el filtro del paso 3.
    ardef = ardef \
        .withColumn("effective_date", F.to_date(F.col("effective_date"), "yyyyMMdd")) \
        .withColumn("valid_until",
                    F.coalesce(
                        F.to_date(F.col("valid_until")),
                        F.lit(file_date_str).cast(DateType())
                    ))
 
    # ── 3. Filtrar rangos válidos para la fecha (post conversión de nulos) ────
    ardef = ardef.filter(
        (F.col("effective_date") <= F.lit(file_date_str)) &
        (F.col("valid_until") >= F.lit(file_date_str))
    )
 
    # ── 4. Convertir claves a numérico ────────────────────────────────────────
    ardef = ardef \
        .withColumn("low_key_for_range", F.col("low_key_for_range").cast(LongType())) \
        .withColumn("table_key", F.col("table_key").cast(LongType()))
 
    # ── 5. Deduplicar por low_key_for_range (version mas reciente gana) ───────
    # Replica exacto la deduplicacion real de legacy (adapters.py, CTE
    # ardef_pre_r): "row_number() over (partition by low_key_for_range order by
    # app_date_valid desc, delete_indicator, high_key_for_range desc)". El
    # desempate por delete_indicator de legacy no hace falta aqui porque el
    # paso 1 ya filtro delete_indicator=' ' -- a diferencia de legacy, que NO
    # filtra rangos borrados en este punto (bug confirmado de legacy, ver
    # gotcha "deleted_range" en .claude/memory/gotchas.md; decidimos no
    # replicarlo).
    #
    # Reemplaza dos pasos de versiones anteriores: un dedup por table_key
    # (Window.partitionBy("table_key")) seguido de un dedup por
    # low_key_for_range SIN desempate real (ordenaba por la misma columna de
    # la particion -- un empate perfecto que Spark resuelve de forma no
    # deterministica). Con datos reales (grupo ARDEF 402824050, ver gotcha
    # "load_visa_ardef() paso 7...") ese dedup viejo podia dejar sobrevivir
    # una version vieja en vez de la mas reciente cuando dos filas compartian
    # low_key_for_range con distinto table_key -- este paso unico lo evita
    # por diseno, con un solo Window y un desempate explicito.
    w_low_key = Window.partitionBy("low_key_for_range").orderBy(
        F.col("effective_date").desc(),
        F.col("table_key").desc()
    )
    ardef = ardef \
        .withColumn("_rn", F.row_number().over(w_low_key)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")

    # ── 6. Rangos solapados: NO se eliminan aqui ───────────────────────────────
    # Versiones anteriores intentaban dejar el ARDEF sin rangos solapados
    # antes de tocar ninguna transaccion (primero con un sweep secuencial
    # buggy, despues con un self-join por effective_date) -- ver gotcha
    # "load_visa_ardef() paso 7..." en .claude/memory/gotchas.md. Un self-join
    # que descarta rangos completos resuelve mal el anidamiento a 3+ niveles
    # con cobertura parcial distinta: puede descartar un rango que sigue
    # siendo el ganador correcto para el subconjunto de cuentas que el rango
    # mas nuevo NO cubre.
    #
    # Ahora replicamos la arquitectura real de legacy: el ARDEF que devuelve
    # esta funcion PUEDE seguir teniendo rangos que se solapan entre si
    # (distinto low_key_for_range, mismo territorio de cuentas). Quien gana
    # se resuelve en join_with_ardef(), UNA VEZ conocida la cuenta real de
    # cada transaccion -- exacto el mismo criterio de legacy (adapters.py,
    # linea ~2382: "row_number() over (partition by app_id, app_hash_file
    # order by app_date_valid desc, high_key_for_range desc)"). Correcto para
    # cualquier profundidad de anidamiento, porque se evalua fresco por cada
    # cuenta en vez de eliminar rangos de forma global.

    # ── 7. Seleccionar campos necesarios ──────────────────────────────────────
    # effective_date se conserva (a diferencia de versiones anteriores, que la
    # descartaban aqui) -- join_with_ardef() la necesita para resolver
    # solapamientos por transaccion (ver paso 6 arriba).
    ardef_fields = [
        "low_key_for_range", "table_key", "account_funding_source",
        "ardef_country",
        # "ardef_region",
        "b2b_program_id", "country",
        "fast_funds", "nnss_indicator", "product_id", "product_subtype",
        "region", "technology_indicator", "travel_indicator", "effective_date"
    ]
    existing_fields = [f for f in ardef_fields if f in ardef.columns]
    ardef = ardef.select(existing_fields)

    # ── 8. Renombrar product_id para evitar conflicto en joins ────────────────
    if "product_id" in ardef.columns:
        ardef = ardef.withColumnRenamed("product_id", "ardef_product_id")

    # ── 9. Cachear — se usa múltiples veces en los joins ─────────────────────
    ardef = ardef.cache()
    count = ardef.count()
    log_info(f"ARDEF loaded: {count:,} valid ranges for date {file_date_str}")
 
    return ardef
 
 
def load_country_table(reference_bucket: str) -> DataFrame:
    """
    Carga la tabla de países.

    Args:
        reference_bucket: Bucket S3 de referencia.

    Returns:
        DataFrame con columnas country_code, visa_region_code.

    Ejemplo:
        load_country_table(reference_bucket)
    """
    country = load_reference_table(reference_bucket, "country")
    return country.select(
        F.col("country_code"),
        F.col("visa_region_code")
    )
 
 
def load_currency_table(reference_bucket: str) -> DataFrame:
    """
    Carga la tabla de monedas.

    Args:
        reference_bucket: Bucket S3 de referencia.

    Returns:
        DataFrame con columnas currency_numeric_code, currency_alphabetic_code.

    Ejemplo:
        load_currency_table(reference_bucket)
    """
    currency = load_reference_table(reference_bucket, "currency")
    return currency.select(
        F.col("currency_numeric_code"),
        F.col("currency_alphabetic_code")
    )


# =============================================================================
# HELPERS: CARGA DE DATOS DEL CLIENTE (DynamoDB)
# =============================================================================
 
def get_client_data(client_id: str, dynamodb_table_client: str) -> dict:
    """
    Obtiene metadatos del cliente desde DynamoDB (tabla client).

    Args:
        client_id: Código de cliente, ej. "EBGR".
        dynamodb_table_client: Nombre de la tabla DynamoDB de clientes.

    Returns:
        Dict con client_id, client_name, local_currency_code,
        settlement_currency_code, report_currency_code,
        issuing_bins_6_digits, issuing_bins_8_digits, acquiring_bins,
        customer_country — o un dict vacío si no existe el cliente (se
        loguea el error, no se relanza).

    Ejemplo:
        get_client_data("EBGR", "itl-0004-itx-dev-dynamo-client-02")
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table_client)
    
    response = table.get_item(Key={'client_id': client_id})
    
    if 'Item' not in response:
        log_error(f"Client not found: {client_id}")
        return {}
    
    item = response['Item']
    log_info(f"  Client found: {item.get('client_name', client_id)}")
    
    return {
        'client_id': client_id,
        'client_name': item.get('client_name', ''),
        'local_currency_code': item.get('local_currency_code', ''),
        'settlement_currency_code': item.get('settlement_currency_code', ''),
        'report_currency_code': item.get('report_currency_code', ''),
        'issuing_bins_6_digits': item.get('issuing_bins_6_digits', ''),
        'issuing_bins_8_digits': item.get('issuing_bins_8_digits', ''),
        'acquiring_bins': item.get('acquiring_bins', ''),
        'customer_country': item.get('customer_country', ''),
    }


# =============================================================================
# HELPER: JOIN CON ARDEF
# =============================================================================
 
def join_with_ardef(df: DataFrame, ardef: DataFrame, account_column: str = "account_number") -> DataFrame:
    """
    Range join hiper-optimizado en PySpark usando bucketing por prefijos.
    Reduce el tiempo de ejecución de minutos a segundos.

    Deriva un prefijo de 3 dígitos del número de cuenta (account_9 / 10^6) y
    del rango ARDEF (explotando cada rango en una fila por cada prefijo que
    abarca), fuerza un broadcast hash join exacto por ese prefijo, y dentro
    del bucket verifica el rango exacto (>=low_key_for_range, <=table_key).

    Como `load_visa_ardef()` ya no elimina rangos que se solapan entre sí
    (ver su docstring), una misma cuenta puede matchear más de un rango ARDEF
    aquí. Tras el join, se resuelve cuál gana con el mismo criterio que usa
    legacy por transacción (`adapters.py`, `ROW_NUMBER() OVER (PARTITION BY
    app_id, app_hash_file ORDER BY app_date_valid DESC, high_key_for_range
    DESC)`): se prioriza `effective_date` más reciente, empate por `table_key`
    más ancho (nuestro equivalente a `high_key_for_range`).

    Args:
        df: DataFrame CLN a enriquecer (BASEII o SMS).
        ardef: DataFrame ARDEF ya cargado (de load_visa_ardef).
        account_column: Nombre de la columna con el número de cuenta/tarjeta
            a usar para el join (default: "account_number"; SMS pasa
            "card_number").

    Returns:
        df con las columnas de ARDEF agregadas (left join — filas sin match
        quedan con esas columnas en null).

    Ejemplo:
        join_with_ardef(df, ardef, "account_number")
    """
    # 1. Crear account_9 (Limpiar asteriscos y tomar los primeros 9 dígitos)
    df = df.withColumn(
        "account_9",
        F.regexp_replace(F.col(account_column), "\\*", "0").substr(1, 9).cast(LongType())
    )
    
    # 🌟 LA MAGIA DE SPARK: CREAR UNA LLAVE EXACTA (PREFIX DE 3 DÍGITOS) 🌟
    # Dividimos entre 1,000,000 para quedarnos con los primeros 3 dígitos (ej. 411)
    df = df.withColumn("join_prefix", F.floor(F.col("account_9") / 1000000).cast(IntegerType()))
 
    # En ARDEF, identificamos desde qué prefijo hasta qué prefijo va el rango
    ardef_opt = ardef.withColumn("prefix_low", F.floor(F.col("low_key_for_range") / 1000000).cast(IntegerType())) \
                     .withColumn("prefix_high", F.floor(F.col("table_key") / 1000000).cast(IntegerType()))
    
    # Si un rango abarca múltiples prefijos (ej. 411 a 412), creamos una fila para cada uno usando sequence y explode
    ardef_opt = ardef_opt.withColumn("join_prefix", F.explode(F.sequence(F.col("prefix_low"), F.col("prefix_high"))))
 
    # Limpiamos posibles columnas solapadas del Clean para evitar ambigüedades
    ardef_cols = ardef.columns
    cols_to_drop = [c for c in ardef_cols if c in df.columns and c != "record"]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)
 
    # 🚀 EL SÚPER JOIN: Primero busca el bucket exacto (O(1)), luego verifica el rango
    df_with_ardef = df.join(
        F.broadcast(ardef_opt),
        on=[
            df["join_prefix"] == ardef_opt["join_prefix"],  # <--- ESTO FUERZA EL BROADCAST HASH JOIN
            df["account_9"] >= ardef_opt["low_key_for_range"],
            df["account_9"] <= ardef_opt["table_key"]
        ],
        how="left"
    )
    
    # Limpieza de columnas temporales (table_key y effective_date se
    # conservan hasta despues del dedup -- las necesita el desempate)
    df_with_ardef = df_with_ardef.drop("low_key_for_range", "account_9", "join_prefix", "prefix_low", "prefix_high")

    # Deduplicación final: una cuenta puede matchear mas de un rango ARDEF
    # solapado (distinto low_key_for_range, mismo territorio -- ver docstring).
    # Se resuelve con el mismo criterio que legacy usa por transaccion:
    # effective_date mas reciente, empate por table_key (rango) mas ancho.
    window_dedup = Window.partitionBy("record").orderBy(
        F.col("effective_date").desc_nulls_last(),
        F.col("table_key").desc_nulls_last()
    )
    df_with_ardef = df_with_ardef.withColumn("_dedup_rank", F.row_number().over(window_dedup))
    df_with_ardef = df_with_ardef.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank")
    df_with_ardef = df_with_ardef.drop("table_key", "effective_date")

    return df_with_ardef


# =============================================================================
# HELPER: CARGAR Y GUARDAR PARQUET
# =============================================================================
 
def load_parquet_safe(path: str) -> DataFrame:
    """
    Carga un archivo Parquet y loguea la cantidad de records leídos.

    Args:
        path: Path S3 completo al Parquet o directorio Parquet.

    Returns:
        DataFrame con el contenido leído.

    Ejemplo:
        load_parquet_safe("s3://bucket/EBGR/VISA/300_baseii_cln_drafts/.../x.parquet")
    """
    df = spark.read.parquet(path)
    count = df.count()
    log_info(f"  Loaded {count:,} records from {path}")
    return df
 
 
def save_parquet(df: DataFrame, path: str):
    """
    Guarda un DataFrame como un único Parquet (coalesce(1)) en la ruta dada.

    Args:
        df: DataFrame a escribir.
        path: Path S3 de destino.

    Returns:
        None.

    Ejemplo:
        save_parquet(result_df, "s3://bucket/EBGR/VISA/400_baseii_cal_drafts/.../x.parquet")
    """
    df.coalesce(1).write.mode("overwrite").parquet(path)
    log_info(f"  Saved to {path}")


# =============================================================================
# CAMPOS CALCULADOS BASEII - GRUPO 1: Campos directos del ARDEF
# =============================================================================
 
def calc_ardef_country(df: DataFrame) -> DataFrame:
    """
    Copia la columna ardef_country del ARDEF (ya unida por join_with_ardef) a
    calc_ardef_country.

    Args:
        df: DataFrame con la columna ardef_country ya presente (post-join
            con ARDEF).

    Returns:
        df con la columna calc_ardef_country agregada.
    """
    return df.withColumn("calc_ardef_country", F.col("ardef_country"))
 
 
def calc_b2b_program_id(df: DataFrame) -> DataFrame:
    """
    Copia la columna b2b_program_id del ARDEF a calc_b2b_program_id.

    Args:
        df: DataFrame con la columna b2b_program_id ya presente (post-join
            con ARDEF).

    Returns:
        df con la columna calc_b2b_program_id agregada.
    """
    return df.withColumn("calc_b2b_program_id", F.col("b2b_program_id"))
 
 
def calc_fast_funds(df: DataFrame) -> DataFrame:
    """
    Copia la columna fast_funds del ARDEF a calc_fast_funds.

    Args:
        df: DataFrame con la columna fast_funds ya presente (post-join con
            ARDEF).

    Returns:
        df con la columna calc_fast_funds agregada.
    """
    return df.withColumn("calc_fast_funds", F.col("fast_funds"))
 
 
def calc_funding_source(df: DataFrame) -> DataFrame:
    """
    Copia la columna account_funding_source del ARDEF a calc_funding_source.

    Args:
        df: DataFrame con la columna account_funding_source ya presente
            (post-join con ARDEF).

    Returns:
        df con la columna calc_funding_source agregada.
    """
    return df.withColumn("calc_funding_source", F.col("account_funding_source"))
 
 
def calc_issuer_country(df: DataFrame) -> DataFrame:
    """
    Copia la columna country del ARDEF (país del emisor) a calc_issuer_country.

    Args:
        df: DataFrame con la columna country ya presente (post-join con
            ARDEF).

    Returns:
        df con la columna calc_issuer_country agregada.
    """
    return df.withColumn("calc_issuer_country", F.col("country"))
 
 
def calc_nnss_indicator(df: DataFrame) -> DataFrame:
    """
    Copia la columna nnss_indicator del ARDEF a calc_nnss_indicator.

    Args:
        df: DataFrame con la columna nnss_indicator ya presente (post-join
            con ARDEF).

    Returns:
        df con la columna calc_nnss_indicator agregada.
    """
    return df.withColumn("calc_nnss_indicator", F.col("nnss_indicator"))
 
 
def calc_product_id_ardef(df: DataFrame) -> DataFrame:
    """
    Copia la columna ardef_product_id (renombrada en load_visa_ardef para
    evitar colisión) a calc_product_id.

    Args:
        df: DataFrame con la columna ardef_product_id ya presente (post-join
            con ARDEF).

    Returns:
        df con la columna calc_product_id agregada.
    """
    return df.withColumn("calc_product_id", F.col("ardef_product_id"))
 
 
def calc_product_subtype(df: DataFrame) -> DataFrame:
    """
    Copia la columna product_subtype del ARDEF a calc_product_subtype.

    Args:
        df: DataFrame con la columna product_subtype ya presente (post-join
            con ARDEF).

    Returns:
        df con la columna calc_product_subtype agregada.
    """
    return df.withColumn("calc_product_subtype", F.col("product_subtype"))
 
 
def calc_technology_indicator(df: DataFrame) -> DataFrame:
    """
    Copia la columna technology_indicator del ARDEF a calc_technology_indicator.

    Args:
        df: DataFrame con la columna technology_indicator ya presente
            (post-join con ARDEF).

    Returns:
        df con la columna calc_technology_indicator agregada.
    """
    return df.withColumn("calc_technology_indicator", F.col("technology_indicator"))
 
 
def calc_travel_indicator(df: DataFrame) -> DataFrame:
    """
    Copia la columna travel_indicator del ARDEF a calc_travel_indicator.

    Args:
        df: DataFrame con la columna travel_indicator ya presente (post-join
            con ARDEF).

    Returns:
        df con la columna calc_travel_indicator agregada.
    """
    return df.withColumn("calc_travel_indicator", F.col("travel_indicator"))


# =============================================================================
# CAMPOS CALCULADOS BASEII - GRUPO 2: String manipulation / coalesce
# =============================================================================
 
def calc_issuer_bin_8(df: DataFrame, account_column: str = "account_number") -> DataFrame:
    """
    Deriva issuer_bin_8: primeros 8 dígitos de la cuenta/tarjeta, con
    asteriscos de máscara reemplazados por '0'.

    Args:
        df: DataFrame de entrada.
        account_column: Columna con el número de cuenta/tarjeta (default:
            "account_number"; SMS pasa "card_number").

    Returns:
        df con la columna calc_issuer_bin_8 agregada.

    Ejemplo:
        calc_issuer_bin_8(df, "account_number")
    """
    return df.withColumn(
        "calc_issuer_bin_8",
        F.regexp_replace(F.col(account_column), "\\*", "0").substr(1, 8)
    )
 
 
def calc_authorization_code_valid_draft(df: DataFrame) -> DataFrame:
    """
    authorization_code_valid para BASEII/draft.
    INVALID si termina en 'x' o si últimos 5 chars están en lista específica.

    Args:
        df: DataFrame con la columna authorization_code.

    Returns:
        df con la columna calc_authorization_code_valid ("VALID"/"INVALID").

    Ejemplo:
        calc_authorization_code_valid_draft(df)
    """
    invalid_suffixes = [" ", "0000", "00000", "0000n", "0000p", "0000y"]
    
    return df.withColumn(
        "calc_authorization_code_valid",
        F.when(
            F.substring(F.col("authorization_code"), -1, 1) == "x",
            F.lit("INVALID")
        ).when(
            F.substring(F.col("authorization_code"), -5, 5).isin(invalid_suffixes),
            F.lit("INVALID")
        ).otherwise(F.lit("VALID"))
    )
 
 
def calc_authorization_code_valid_sms(df: DataFrame) -> DataFrame:
    """
    authorization_code_valid para SMS usando authorization_id_resp._code

    Args:
        df: DataFrame con la columna authorization_id_resp_code.

    Returns:
        df con la columna calc_authorization_code_valid ("VALID"/"INVALID").

    Ejemplo:
        calc_authorization_code_valid_sms(df)
    """
    invalid_suffixes = [" ", "0000", "00000", "0000n", "0000p", "0000y"]
    col_name = "authorization_id_resp_code"
    
    return df.withColumn(
        "calc_authorization_code_valid",
        F.when(
            F.substring(F.col(col_name), -1, 1) == "x",
            F.lit("INVALID")
        ).when(
            F.substring(F.col(col_name), -5, 5).isin(invalid_suffixes),
            F.lit("INVALID")
        ).otherwise(F.lit("VALID"))
    )
 
 
def calc_business_application_id(df: DataFrame) -> DataFrame:
    """
    Coalesce de business_application_id_fl, _cr, _ft

    Args:
        df: DataFrame con las columnas business_application_id_fl/_cr/_ft.

    Returns:
        df con la columna calc_business_application_id (primer valor no
        vacío entre las 3 variantes).

    Ejemplo:
        calc_business_application_id(df)
    """
    return df.withColumn(
        "calc_business_application_id",
        F.coalesce(
            F.when(F.trim(F.col("business_application_id_fl")) != "", F.trim(F.col("business_application_id_fl"))),
            F.when(F.trim(F.col("business_application_id_cr")) != "", F.trim(F.col("business_application_id_cr"))),
            F.when(F.trim(F.col("business_application_id_ft")) != "", F.trim(F.col("business_application_id_ft")))
        )
    )
 
 
def calc_business_format_code(df: DataFrame) -> DataFrame:
    """
    Coalesce de business_format_code_cr, _fl, _ft, _df, _pd, _sd, _sp

    Args:
        df: DataFrame con las 7 variantes de business_format_code.

    Returns:
        df con la columna calc_business_format_code (primer valor no vacío).

    Ejemplo:
        calc_business_format_code(df)
    """
    return df.withColumn(
        "calc_business_format_code",
        F.coalesce(
            F.when(F.trim(F.col("business_format_code_cr")) != "", F.trim(F.col("business_format_code_cr"))),
            F.when(F.trim(F.col("business_format_code_fl")) != "", F.trim(F.col("business_format_code_fl"))),
            F.when(F.trim(F.col("business_format_code_ft")) != "", F.trim(F.col("business_format_code_ft"))),
            F.when(F.trim(F.col("business_format_code_df")) != "", F.trim(F.col("business_format_code_df"))),
            F.when(F.trim(F.col("business_format_code_pd")) != "", F.trim(F.col("business_format_code_pd"))),
            F.when(F.trim(F.col("business_format_code_sd")) != "", F.trim(F.col("business_format_code_sd"))),
            F.when(F.trim(F.col("business_format_code_sp")) != "", F.trim(F.col("business_format_code_sp")))
        )
    )
 
 
def calc_message_reason_code(df: DataFrame) -> DataFrame:
    """
    Coalesce de message_reason_code_df, _sd, _sp

    Args:
        df: DataFrame con las 3 variantes de message_reason_code.

    Returns:
        df con la columna calc_message_reason_code (primer valor no vacío).

    Ejemplo:
        calc_message_reason_code(df)
    """
    return df.withColumn(
        "calc_message_reason_code",
        F.coalesce(
            F.when(F.trim(F.col("message_reason_code_df")) != "", F.trim(F.col("message_reason_code_df"))),
            F.when(F.trim(F.col("message_reason_code_sd")) != "", F.trim(F.col("message_reason_code_sd"))),
            F.when(F.trim(F.col("message_reason_code_sp")) != "", F.trim(F.col("message_reason_code_sp")))
        )
    )
 
 
def calc_network_identification_code(df: DataFrame) -> DataFrame:
    """
    Coalesce de network_identification_code_df, _sd, _sp

    Args:
        df: DataFrame con las 3 variantes de network_identification_code.

    Returns:
        df con la columna calc_network_identification_code (primer valor no
        vacío).

    Ejemplo:
        calc_network_identification_code(df)
    """
    return df.withColumn(
        "calc_network_identification_code",
        F.coalesce(
            F.when(F.trim(F.col("network_identification_code_df")) != "", F.trim(F.col("network_identification_code_df"))),
            F.when(F.trim(F.col("network_identification_code_sd")) != "", F.trim(F.col("network_identification_code_sd"))),
            F.when(F.trim(F.col("network_identification_code_sp")) != "", F.trim(F.col("network_identification_code_sp")))
        )
    )


def calc_token_requestor_id_draft(df: DataFrame) -> DataFrame:
    """
    token_requestor_id para BASEII/draft a partir de las variantes SD/SP.
    BLANK si ambas son null; VALID si alguna tiene un id no-cero; INVALID en
    el resto (presentes pero en cero).

    Args:
        df: DataFrame con las columnas token_requestor_id_sd/_sp.

    Returns:
        df con la columna calc_token_requestor_id ("BLANK"/"VALID"/"INVALID").

    Ejemplo:
        calc_token_requestor_id_draft(df)
    """
    sd = F.col("token_requestor_id_sd")
    sp = F.col("token_requestor_id_sp")
    return df.withColumn(
        "calc_token_requestor_id",
        F.when(sd.isNull() & sp.isNull(), F.lit("BLANK"))
         .when(
             F.coalesce(F.when(sd != 0, sd), F.when(sp != 0, sp)).isNotNull(),
             F.lit("VALID")
         )
         .otherwise(F.lit("INVALID"))
    )


def calc_type_of_purchase(df: DataFrame) -> DataFrame:
    """
    Coalesce de type_of_purchase_fl, _ft

    Args:
        df: DataFrame con las 2 variantes de type_of_purchase.

    Returns:
        df con la columna calc_type_of_purchase (primer valor no vacío).

    Ejemplo:
        calc_type_of_purchase(df)
    """
    return df.withColumn(
        "calc_type_of_purchase",
        F.coalesce(
            F.when(F.trim(F.col("type_of_purchase_fl")) != "", F.trim(F.col("type_of_purchase_fl"))),
            F.when(F.trim(F.col("type_of_purchase_ft")) != "", F.trim(F.col("type_of_purchase_ft")))
        )
    )
 
 
def calc_surcharge_amount(df: DataFrame) -> DataFrame:
    """
    MAX de surcharge_amount_df, _sd, _sp

    Args:
        df: DataFrame con las 3 variantes de surcharge_amount.

    Returns:
        df con la columna calc_surcharge_amount (double, máximo de las 3,
        tratando null como 0.0).

    Ejemplo:
        calc_surcharge_amount(df)
    """
    return df.withColumn(
        "calc_surcharge_amount",
        F.greatest(
            F.coalesce(F.col("surcharge_amount_df"), F.lit(0.0)),
            F.coalesce(F.col("surcharge_amount_sd"), F.lit(0.0)),
            F.coalesce(F.col("surcharge_amount_sp"), F.lit(0.0))
        ).cast(DoubleType())
    )


# =============================================================================
# CAMPOS CALCULADOS BASEII - GRUPO 3: Lógica condicional
# =============================================================================
 
def calc_business_mode_draft(df: DataFrame, file_type: str) -> DataFrame:
    """
    business_mode para BASEII/draft.
    Basado en draft_code y file_type.

    Nota: usa literales en MAYÚSCULA ("ACQUIRING"/"ISSUING") — a diferencia
    de Mastercard, que usa minúscula para el mismo concepto (ver gotchas.md →
    "business_mode: MAYÚSCULA en Visa, minúscula en Mastercard").

    Args:
        df: DataFrame con la columna draft_code.
        file_type: "IN" u "OUT" — invierte el mapeo acquiring/issuing según
            la dirección del archivo.

    Returns:
        df con la columna calc_business_mode ("ACQUIRING"/"ISSUING"/"").

    Ejemplo:
        calc_business_mode_draft(df, "IN")
    """
    acquiring_codes_out = ["05", "25", "06", "26", "07", "27"]
    issuing_codes_out = ["15", "35", "16", "36", "17", "37"]
    
    if file_type == "OUT":
        return df.withColumn(
            "calc_business_mode",
            F.when(F.col("draft_code").isin(acquiring_codes_out), F.lit("ACQUIRING"))
             .when(F.col("draft_code").isin(issuing_codes_out), F.lit("ISSUING"))
             .otherwise(F.lit(""))
        )
    else:  # IN
        return df.withColumn(
            "calc_business_mode",
            F.when(F.col("draft_code").isin(acquiring_codes_out), F.lit("ISSUING"))
             .when(F.col("draft_code").isin(issuing_codes_out), F.lit("ACQUIRING"))
             .otherwise(F.lit(""))
        )


def calc_business_mode_sms(df: DataFrame) -> DataFrame:
    """
    business_mode para SMS basado en issuer_acquirer_indicator.

    Args:
        df: DataFrame con la columna issuer_acquirer_indicator.

    Returns:
        df con la columna calc_business_mode ("ACQUIRING"/"ISSUING"/"").

    Ejemplo:
        calc_business_mode_sms(df)
    """
    return df.withColumn(
        "calc_business_mode",
        F.when(F.col("issuer_acquirer_indicator") == "A", F.lit("ACQUIRING"))
         .when(F.col("issuer_acquirer_indicator") == "I", F.lit("ISSUING"))
         .otherwise(F.lit(""))
    )


def calc_business_transaction_type_draft(df: DataFrame, file_type: str) -> DataFrame:
    """
    business_transaction_type para BASEII/draft.
    Lógica compleja basada en draft_code, MCC, usage_code, etc.

    Args:
        df: DataFrame con draft_code, merchant_category_code, usage_code,
            special_condition_indicator_merchant_draft_indicator,
            draft_code_qualifier_0.
        file_type: "IN" u "OUT" — la lógica de purchase/ATM/cash difiere
            según dirección.

    Returns:
        df con la columna calc_business_transaction_type (int; 255 si
        ninguna condición matchea).

    Ejemplo:
        calc_business_transaction_type_draft(df, "IN")
    """
    purchase_codes = ["05", "15", "25", "35"]
    cash_codes     = ["06", "16", "26", "36"]
    atm_codes      = ["07", "17", "27", "37"]
    special_mcc    = [4829, 6051, 7995]

    is_in    = F.lit(file_type == "IN")
    dc       = F.col("draft_code")
    mcc      = F.col("merchant_category_code")
    usage    = F.col("usage_code")
    sci      = F.col("special_condition_indicator_merchant_draft_indicator")
    qualifier = F.col("draft_code_qualifier_0")

    return df.withColumn(
        "calc_business_transaction_type",
        # --- IN: purchase codes ---
        F.when(dc.isin(purchase_codes) & is_in & sci.isin(["7", "8"]),        F.lit(3))
        .when(dc.isin(purchase_codes) & is_in & ~sci.isin(["7", "8"]),        F.lit(1))
        # --- IN: ATM codes ---
        .when(dc.isin(atm_codes) & is_in & (mcc == 6010),                     F.lit(21))
        .when(dc.isin(atm_codes) & is_in & (mcc == 6011),                     F.lit(22))
        # --- IN: cash codes ---
        .when(dc.isin(cash_codes) & is_in & (usage == 1) & (qualifier == 2),  F.lit(25))
        .when(dc.isin(cash_codes) & is_in & (usage == 1),                     F.lit(19))
        # --- OUT: purchase codes ---
        .when(dc.isin(purchase_codes) & ~is_in & ~mcc.isin(special_mcc),      F.lit(1))
        .when(dc.isin(purchase_codes) & ~is_in & mcc.isin(special_mcc),       F.lit(3))
        # --- OUT: ATM codes ---
        .when(dc.isin(atm_codes) & ~is_in & (mcc == 6010),                    F.lit(21))
        .when(dc.isin(atm_codes) & ~is_in & (mcc == 6011),                    F.lit(22))
        # --- OUT: cash codes ---
        .when(dc.isin(cash_codes) & ~is_in & (usage == 1) & (qualifier == 2), F.lit(25))
        .when(dc.isin(cash_codes) & ~is_in & (usage == 1),                    F.lit(19))
        .otherwise(F.lit(255)).cast(IntegerType())
    )


def calc_business_transaction_cycle_draft(df: DataFrame) -> DataFrame:
    """
    business_transaction_cycle para BASEII/draft.
    Lógica basada en draft_code (transaction_code) y usage_code.

    Args:
        df: DataFrame con draft_code y usage_code.

    Returns:
        df con la columna calc_business_transaction_cycle (int; 255 si
        ninguna condición matchea).

    Ejemplo:
        calc_business_transaction_cycle_draft(df)
    """
    purchase_codes  = ["05", "06", "07"]
    reversal_codes  = ["15", "16", "17", "35", "36", "37"]
    chargeback_codes = ["25", "26", "27"]

    dc    = F.col("draft_code")
    usage = F.col("usage_code")

    return df.withColumn(
        "calc_business_transaction_cycle",
        F.when(
            dc.isin(purchase_codes),
            F.when(usage == 1, F.lit(11))
             .when(usage == 2, F.lit(23))
             .when(usage == 9, F.lit(6))
             .otherwise(F.lit(255))
        ).when(
            dc.isin(reversal_codes),
            F.when(usage == 1, F.lit(1))
             .when(usage == 9, F.lit(4))
             .otherwise(F.lit(255))
        ).when(
            dc.isin(chargeback_codes),
            F.when(usage == 1, F.lit(11))
             .when(usage == 9, F.lit(6))
             .when(usage == 2, F.lit(25))
             .otherwise(F.lit(255))
        ).otherwise(F.lit(255)).cast(IntegerType())
    )


def calc_business_transaction_type_sms(df: DataFrame) -> DataFrame:
    """
    business_transaction_type para SMS.

    Clasifica según combinación de request_message_type/response_code
    (éxito/rechazo), processing_code, pos_condition_code y
    merchant's_type (MCC).

    Args:
        df: DataFrame con request_message_type, response_code,
            processing_code, pos_condition_code, `merchant's_type`.

    Returns:
        df con la columna calc_business_transaction_type (int, o NULL si
        ninguna condición matchea).

    Ejemplo:
        calc_business_transaction_type_sms(df)
    """
    df = df.withColumn("_rmt", F.col("request_message_type"))
    df = df.withColumn("_rc", F.col("response_code"))
    df = df.withColumn("_pc", F.substring(F.col("processing_code"), 1, 2))
    df = df.withColumn("_pos", F.col("pos_condition_code"))
    df = df.withColumn("_mcc", F.col("`merchant's_type`"))
    
    cond_success = F.col("_rmt").isin(["0200", "0220", "0400", "0420"]) & (F.col("_rc") == "00")
    cond_decline = F.col("_rmt").isin(["0200", "0220", "0400", "0420"]) & (F.col("_rc") != "00")
    not_special_pos = ~F.col("_pos").isin(["13", "51"])
    not_special_mcc = ~F.col("_mcc").isin([4815, 6010, 6011])
    
    df = df.withColumn(
        "calc_business_transaction_type",
        F.when(cond_success & (F.col("_pc") == "00") & not_special_pos & not_special_mcc, F.lit(1))
         .when(cond_success & (F.col("_pc") == "01") & not_special_pos & (F.col("_mcc") == 6010), F.lit(21))
         .when(cond_success & (F.col("_pc") == "01") & not_special_pos & (F.col("_mcc") == 6011), F.lit(22))
         .when(cond_success & (F.col("_pc") == "10") & not_special_pos & not_special_mcc, F.lit(30))
         .when(cond_success & (F.col("_pc") == "11") & not_special_pos & not_special_mcc, F.lit(3))
         .when(cond_success & (F.col("_pc") == "19") & not_special_pos & not_special_mcc, F.lit(115))
         .when(cond_success & (F.col("_pc") == "20") & not_special_pos & not_special_mcc, F.lit(19))
         .when(cond_success & (F.col("_pc") == "22") & F.col("_pos").isin(["13"]) & not_special_mcc, F.lit(20))
         .when(cond_success & (F.col("_pc") == "26") & not_special_pos & not_special_mcc, F.lit(25))
         .when(cond_success & (F.col("_pc") == "29") & not_special_pos & not_special_mcc, F.lit(200))
         .when(cond_success & (F.col("_pc") == "30") & not_special_pos & (F.col("_mcc") == 6011), F.lit(247))
         .when(cond_success & (F.col("_pc") == "40") & not_special_pos & (F.col("_mcc") == 6011), F.lit(250))
         .when(cond_success & (F.col("_pc") == "50") & not_special_pos & not_special_mcc, F.lit(27))
         .when(cond_decline & (F.col("_mcc") != 6011), F.lit(236))
         .when(cond_decline & (F.col("_mcc") == 6011), F.lit(249))
         .otherwise(F.lit(None).cast(IntegerType()))
    )
    
    df = df.drop("_rmt", "_rc", "_pc", "_pos", "_mcc")
    return df


def calc_business_transaction_cycle_sms(df: DataFrame) -> DataFrame:
    """
    business_transaction_cycle para SMS.
    No aplica para SMS (la lógica es exclusiva de transaction_code BASEII/draft) —
    en legacy el campo se mantiene en la tabla como NULL.

    Args:
        df: DataFrame de entrada (no se lee ninguna columna).

    Returns:
        df con la columna calc_business_transaction_cycle siempre NULL.

    Ejemplo:
        calc_business_transaction_cycle_sms(df)
    """
    return df.withColumn("calc_business_transaction_cycle", F.lit(None).cast(IntegerType()))


def calc_reversal_indicator_draft(df: DataFrame) -> DataFrame:
    """
    reversal_indicator para BASEII/draft.

    Args:
        df: DataFrame con la columna draft_code.

    Returns:
        df con la columna calc_reversal_indicator (1 si draft_code es un
        código de reversal/chargeback, 0 en caso contrario).

    Ejemplo:
        calc_reversal_indicator_draft(df)
    """
    reversal_codes = ["25", "26", "27", "35", "36", "37"]
    return df.withColumn(
        "calc_reversal_indicator",
        F.when(F.col("draft_code").isin(reversal_codes), F.lit(1)).otherwise(F.lit(0)).cast(IntegerType())
    )
 
 
def calc_reversal_indicator_sms(df: DataFrame) -> DataFrame:
    """
    reversal_indicator para SMS.

    Args:
        df: DataFrame con request_message_type y response_code.

    Returns:
        df con la columna calc_reversal_indicator (0 para autorización
        exitosa, 1 para reversal exitoso, 0 en cualquier otro caso).

    Ejemplo:
        calc_reversal_indicator_sms(df)
    """
    return df.withColumn(
        "calc_reversal_indicator",
        F.when(
            F.col("request_message_type").isin(["0200", "0220"]) & (F.col("response_code") == "00"),
            F.lit(0)
        ).when(
            F.col("request_message_type").isin(["0400", "0420"]) & (F.col("response_code") == "00"),
            F.lit(1)
        ).otherwise(F.lit(0)).cast(IntegerType())
    )
 
 
def calc_jurisdiction_country_draft(df: DataFrame) -> DataFrame:
    """
    jurisdiction_country para BASEII/draft = merchant_country_code

    Args:
        df: DataFrame con la columna merchant_country_code.

    Returns:
        df con la columna calc_jurisdiction_country.

    Ejemplo:
        calc_jurisdiction_country_draft(df)
    """
    return df.withColumn("calc_jurisdiction_country", F.col("merchant_country_code"))
 
 
def calc_jurisdiction_country_sms(df: DataFrame) -> DataFrame:
    """
    jurisdiction_country para SMS = card_acceptor_country

    Args:
        df: DataFrame con la columna card_acceptor_country.

    Returns:
        df con la columna calc_jurisdiction_country.

    Ejemplo:
        calc_jurisdiction_country_sms(df)
    """
    return df.withColumn("calc_jurisdiction_country", F.col("card_acceptor_country"))


# =============================================================================
# CAMPOS CALCULADOS BASEII - GRUPO 4: JOINs con country/currency
# =============================================================================
 
def calc_issuer_region(df: DataFrame, country_df: DataFrame) -> DataFrame:
    """
    issuer_region: JOIN country del ARDEF con tabla country.

    Args:
        df: DataFrame con la columna country (país emisor del ARDEF).
        country_df: DataFrame de referencia country (código→visa_region_code).

    Returns:
        df con la columna calc_issuer_region agregada (left join — null si
        el país no matchea en la tabla de referencia).

    Ejemplo:
        calc_issuer_region(df, country_df)
    """
    country_for_issuer = country_df.select(
        F.col("country_code").alias("_country_code_issuer"),
        F.col("visa_region_code").alias("calc_issuer_region")
    )
    
    df = df.join(
        F.broadcast(country_for_issuer),
        F.col("country") == F.col("_country_code_issuer"),
        how="left"
    ).drop("_country_code_issuer")
    
    return df
 
 
def calc_jurisdiction_region_draft(df: DataFrame, country_df: DataFrame) -> DataFrame:
    """
    jurisdiction_region para BASEII/draft: JOIN merchant_country_code.

    Args:
        df: DataFrame con la columna merchant_country_code.
        country_df: DataFrame de referencia country.

    Returns:
        df con la columna calc_jurisdiction_region agregada.

    Ejemplo:
        calc_jurisdiction_region_draft(df, country_df)
    """
    country_for_merchant = country_df.select(
        F.col("country_code").alias("_country_code_merchant"),
        F.col("visa_region_code").alias("calc_jurisdiction_region")
    )
    
    df = df.join(
        F.broadcast(country_for_merchant),
        F.col("merchant_country_code") == F.col("_country_code_merchant"),
        how="left"
    ).drop("_country_code_merchant")
    
    return df
 
 
def calc_jurisdiction_region_sms(df: DataFrame, country_df: DataFrame) -> DataFrame:
    """
    jurisdiction_region para SMS: JOIN card_acceptor_country.

    Args:
        df: DataFrame con la columna card_acceptor_country.
        country_df: DataFrame de referencia country.

    Returns:
        df con la columna calc_jurisdiction_region agregada.

    Ejemplo:
        calc_jurisdiction_region_sms(df, country_df)
    """
    country_for_merchant = country_df.select(
        F.col("country_code").alias("_country_code_merchant"),
        F.col("visa_region_code").alias("calc_jurisdiction_region")
    )
    
    df = df.join(
        F.broadcast(country_for_merchant),
        F.col("card_acceptor_country") == F.col("_country_code_merchant"),
        how="left"
    ).drop("_country_code_merchant")
    
    return df
 
 
def calc_source_currency_code_alphabetic_draft(df: DataFrame, currency_df: DataFrame) -> DataFrame:
    """
    source_currency_code_alphabetic para BASEII/draft.

    Args:
        df: DataFrame con la columna source_currency_code.
        currency_df: DataFrame de referencia currency.

    Returns:
        df con la columna calc_source_currency_code_alphabetic agregada.

    Ejemplo:
        calc_source_currency_code_alphabetic_draft(df, currency_df)
    """
    currency_lookup = currency_df.select(
        F.col("currency_numeric_code").alias("_currency_numeric"),
        F.col("currency_alphabetic_code").alias("calc_source_currency_code_alphabetic")
    )
    
    df = df.join(
        F.broadcast(currency_lookup),
        F.col("source_currency_code") == F.col("_currency_numeric"),
        how="left"
    ).drop("_currency_numeric")
    
    return df
 
 
def calc_source_currency_code_alphabetic_sms(df: DataFrame, currency_df: DataFrame) -> DataFrame:
    """
    source_currency_code_alphabetic para SMS usando draft_currency_code.

    Args:
        df: DataFrame con la columna draft_currency_code.
        currency_df: DataFrame de referencia currency.

    Returns:
        df con la columna calc_source_currency_code_alphabetic agregada.

    Ejemplo:
        calc_source_currency_code_alphabetic_sms(df, currency_df)
    """
    currency_lookup = currency_df.select(
        F.col("currency_numeric_code").alias("_currency_numeric"),
        F.col("currency_alphabetic_code").alias("calc_source_currency_code_alphabetic")
    )
    
    df = df.join(
        F.broadcast(currency_lookup),
        F.col("draft_currency_code") == F.col("_currency_numeric"),
        how="left"
    ).drop("_currency_numeric")
    
    return df


# =============================================================================
# JURISDICTION - DRAFT
# =============================================================================
 
def calc_jurisdiction_draft(df: DataFrame, country_df: DataFrame, file_type: str, client_data: dict) -> DataFrame:
    """
    jurisdiction para BASEII/draft.
    Replica exacta de la lógica original en Pandas.

    Agrega merchant_region_code y ardef_region (joins contra country_df) y
    clasifica cada transacción en on-us/off-us/intraregional/interregional
    según el país/región del comercio vs. el país/región del ARDEF y el
    matching de BINs propios del cliente (issuing_bins_6/8_digits,
    acquiring_bins de DynamoDB) — la lógica de qué BINs importan (issuing vs
    acquiring) se invierte según file_type.

    Args:
        df: DataFrame con merchant_country_code, ardef_country,
            account_number, account_reference_number_acquiring_identifier,
            collection_only_flag.
        country_df: DataFrame de referencia country.
        file_type: "IN" u "OUT" — determina si on-us se evalúa con BINs de
            issuing o de acquiring.
        client_data: Dict de configuración del cliente (de get_client_data),
            con issuing_bins_6_digits, issuing_bins_8_digits, acquiring_bins.

    Returns:
        df con calc_jurisdiction agregada, más merchant_region_code y
        ardef_region como columnas adicionales (subproductos de los joins).

    Ejemplo:
        calc_jurisdiction_draft(df, country_df, "IN", client_data)
    """
    issuing_bins_6 = [b.strip() for b in str(client_data.get('issuing_bins_6_digits', '')).split(',') if b.strip()]
    issuing_bins_8 = [b.strip() for b in str(client_data.get('issuing_bins_8_digits', '')).split(',') if b.strip()]
    acquiring_bins = [b.strip() for b in str(client_data.get('acquiring_bins', '')).split(',') if b.strip()]
    
    # JOIN para obtener merchant_region_code
    country_merchant = country_df.select(
        F.col("country_code").alias("_mc_country"),
        F.col("visa_region_code").alias("merchant_region_code")
    )
    df = df.join(
        F.broadcast(country_merchant),
        F.col("merchant_country_code") == F.col("_mc_country"),
        how="left"
    ).drop("_mc_country")
    
    # JOIN para obtener ardef_region
    country_ardef = country_df.select(
        F.col("country_code").alias("_ac_country"),
        F.col("visa_region_code").alias("ardef_region")
    )
    df = df.join(
        F.broadcast(country_ardef),
        F.col("ardef_country") == F.col("_ac_country"),
        how="left"
    ).drop("_ac_country")
    
    # Columnas auxiliares para BIN matching
    df = df.withColumn(
        "_account_6",
        F.regexp_replace(F.col("account_number"), "\\*", "0").substr(1, 6)
    ).withColumn(
        "_account_8",
        F.regexp_replace(F.col("account_number"), "\\*", "0").substr(1, 8)
    ).withColumn(
        "_acq_id_padded",
        F.lpad(F.col("account_reference_number_acquiring_identifier").cast(StringType()), 6, "0")
    )
    
    # Condiciones base
    same_country = F.col("merchant_country_code") == F.col("ardef_country")
    collection_flag = F.col("collection_only_flag") == "C"
    
    # Matching de BINs
    issuing_6_match = F.col("_account_6").isin(issuing_bins_6) if issuing_bins_6 else F.lit(False)
    issuing_8_match = F.col("_account_8").isin(issuing_bins_8) if issuing_bins_8 else F.lit(False)
    acquiring_match = F.col("_acq_id_padded").isin(acquiring_bins) if acquiring_bins else F.lit(False)
    
    # Lógica según file_type (exactamente como el original)
    if file_type == "OUT":
        on_us_condition = same_country & (collection_flag | issuing_6_match | issuing_8_match)
    else:  # IN
        on_us_condition = same_country & (collection_flag | acquiring_match)
    
    # Aplicar condiciones en orden
    df = df.withColumn(
        "calc_jurisdiction",
        F.when(on_us_condition, F.lit("on-us"))
         .when(same_country, F.lit("off-us"))
         .when((~same_country) & (F.col("merchant_region_code") == F.col("ardef_region")), F.lit("intraregional"))
         .when((~same_country) & (F.col("merchant_region_code") != F.col("ardef_region")), F.lit("interregional"))
         .otherwise(F.lit(""))
    )
    
    df = df.drop("_account_6", "_account_8", "_acq_id_padded")
    
    return df


def calc_jurisdiction_assigned_draft(df: DataFrame) -> DataFrame:
    """
    jurisdiction_assigned para BASEII/draft.
    Lógica exacta del original.

    Args:
        df: DataFrame con merchant_country_code, ardef_country,
            merchant_region_code, ardef_region (las 2 últimas agregadas por
            calc_jurisdiction_draft).

    Returns:
        df con la columna calc_jurisdiction_assigned (país si mismo país,
        región si distinto país mismo región, "9" si distinta región, ""
        si ninguna condición matchea).

    Ejemplo:
        calc_jurisdiction_assigned_draft(df)
    """
    return df.withColumn(
        "calc_jurisdiction_assigned",
        F.when(
            F.col("merchant_country_code") == F.col("ardef_country"),
            F.col("merchant_country_code")
        ).when(
            (F.col("merchant_country_code") != F.col("ardef_country")) &
            (F.col("merchant_region_code") == F.col("ardef_region")),
            F.col("ardef_region")
        ).when(
            (F.col("merchant_country_code") != F.col("ardef_country")) &
            (F.col("merchant_region_code") != F.col("ardef_region")),
            F.lit("9")
        ).otherwise(F.lit(""))
    )


def calc_settlement_report_currency_code_draft(df: DataFrame, client_data: dict) -> DataFrame:
    """
    settlement_report_currency_code para BASEII/draft.
    Si jurisdiction in (on-us, off-us) y settlement_flag != 0 -> local_currency_code del cliente.
    Caso contrario -> settlement_currency_code del cliente.

    Args:
        df: DataFrame con calc_jurisdiction y settlement_flag.
        client_data: Dict de configuración del cliente, con
            local_currency_code y settlement_currency_code.

    Returns:
        df con la columna calc_settlement_report_currency_code.

    Ejemplo:
        calc_settlement_report_currency_code_draft(df, client_data)
    """
    local_ccy      = str(client_data.get('local_currency_code', '')).strip()
    settlement_ccy = str(client_data.get('settlement_currency_code', '')).strip()

    return df.withColumn(
        "calc_settlement_report_currency_code",
        F.when(
            F.col("calc_jurisdiction").isin(["on-us", "off-us"]) & (F.col("settlement_flag") != 0),
            F.lit(local_ccy)
        ).otherwise(F.lit(settlement_ccy))
    )


# =============================================================================
# JURISDICTION - SMS
# =============================================================================
 
def calc_jurisdiction_sms(df: DataFrame, country_df: DataFrame, client_data: dict) -> DataFrame:
    """
    jurisdiction para SMS.
    NOTA: El original hace merge left_on='card_acceptor_country', right_on='merchant_country_code'
    Esto AGREGA merchant_country_code como columna al DataFrame.

    Mismo criterio de clasificación que calc_jurisdiction_draft, pero
    el on-us tiene 2 ramas según issuer_acquirer_indicator ("A" usa BINs de
    issuing, "I" usa BINs de acquiring) — en vez de depender de file_type.

    Args:
        df: DataFrame con card_acceptor_country, ardef_country, card_number,
            acquiring_institution_id_1, issuer_acquirer_indicator.
        country_df: DataFrame de referencia country.
        client_data: Dict de configuración del cliente, con
            issuing_bins_6_digits, issuing_bins_8_digits, acquiring_bins.

    Returns:
        df con calc_jurisdiction agregada, más merchant_country_code,
        merchant_region_code y ardef_region como columnas adicionales.

    Ejemplo:
        calc_jurisdiction_sms(df, country_df, client_data)
    """
    issuing_bins_6 = [b.strip() for b in str(client_data.get('issuing_bins_6_digits', '')).split(',') if b.strip()]
    issuing_bins_8 = [b.strip() for b in str(client_data.get('issuing_bins_8_digits', '')).split(',') if b.strip()]
    acquiring_bins = [b.strip() for b in str(client_data.get('acquiring_bins', '')).split(',') if b.strip()]
    
    # JOIN para obtener merchant_country_code y merchant_region_code
    # Simula: pd.merge(source, country, left_on="card_acceptor_country", right_on="merchant_country_code")
    country_merchant = country_df.select(
        F.col("country_code").alias("merchant_country_code"),
        F.col("visa_region_code").alias("merchant_region_code")
    )
    df = df.join(
        F.broadcast(country_merchant),
        F.col("card_acceptor_country") == F.col("merchant_country_code"),
        how="left"
    )
    
    # JOIN para obtener ardef_region
    country_ardef = country_df.select(
        F.col("country_code").alias("_ac_country"),
        F.col("visa_region_code").alias("ardef_region")
    )
    df = df.join(
        F.broadcast(country_ardef),
        F.col("ardef_country") == F.col("_ac_country"),
        how="left"
    ).drop("_ac_country")
    
    # Columnas auxiliares
    df = df.withColumn(
        "_card_6",
        F.regexp_replace(F.col("card_number"), "\\*", "0").substr(1, 6)
    ).withColumn(
        "_card_8",
        F.regexp_replace(F.col("card_number"), "\\*", "0").substr(1, 8)
    ).withColumn(
        "_acq_id_padded",
        F.lpad(F.col("acquiring_institution_id_1").cast(StringType()), 6, "0")
    )
    
    # Condiciones
    same_country = F.col("merchant_country_code") == F.col("ardef_country")
    
    issuing_6_match = F.col("_card_6").isin(issuing_bins_6) if issuing_bins_6 else F.lit(False)
    issuing_8_match = F.col("_card_8").isin(issuing_bins_8) if issuing_bins_8 else F.lit(False)
    acquiring_match = F.col("_acq_id_padded").isin(acquiring_bins) if acquiring_bins else F.lit(False)
    
    # on-us para Acquiring (indicator=A): usa issuing BINs
    # on-us para Issuing (indicator=I): usa acquiring BINs
    on_us_acquiring = same_country & (F.col("issuer_acquirer_indicator") == "A") & (issuing_6_match | issuing_8_match)
    on_us_issuing = same_country & (F.col("issuer_acquirer_indicator") == "I") & acquiring_match
    
    df = df.withColumn(
        "calc_jurisdiction",
        F.when(on_us_acquiring | on_us_issuing, F.lit("on-us"))
         .when(same_country, F.lit("off-us"))
         .when((~same_country) & (F.col("merchant_region_code") == F.col("ardef_region")), F.lit("intraregional"))
         .when((~same_country) & (F.col("merchant_region_code") != F.col("ardef_region")), F.lit("interregional"))
         .otherwise(F.lit(""))
    )
    
    df = df.drop("_card_6", "_card_8", "_acq_id_padded")
    
    return df


def calc_jurisdiction_assigned_sms(df: DataFrame) -> DataFrame:
    """
    jurisdiction_assigned para SMS.
    Usa merchant_country_code (agregado por el JOIN en calc_jurisdiction_sms).

    Args:
        df: DataFrame con merchant_country_code, ardef_country,
            merchant_region_code, ardef_region.

    Returns:
        df con la columna calc_jurisdiction_assigned (mismo criterio que
        calc_jurisdiction_assigned_draft).

    Ejemplo:
        calc_jurisdiction_assigned_sms(df)
    """
    return df.withColumn(
        "calc_jurisdiction_assigned",
        F.when(
            F.col("merchant_country_code") == F.col("ardef_country"),
            F.col("merchant_country_code")
        ).when(
            (F.col("merchant_country_code") != F.col("ardef_country")) &
            (F.col("merchant_region_code") == F.col("ardef_region")),
            F.col("ardef_region")
        ).when(
            (F.col("merchant_country_code") != F.col("ardef_country")) &
            (F.col("merchant_region_code") != F.col("ardef_region")),
            F.lit("9")
        ).otherwise(F.lit(""))
    )


# =============================================================================
# TIMELINESS
# =============================================================================
 
def calc_timeliness_draft(df: DataFrame) -> DataFrame:
    """
    timeliness para BASEII/draft.
    Dias no-domingo en el intervalo abierto (purchase_date, central_processing_date).
    Formula: total_days - 1 - sundays_in_window
    sundays_in_window = max(0, floor((window_size + 6 - offset) / 7))
      donde window_size = total_days - 1
            offset = dias desde purchase+1 hasta el primer domingo (0 si purchase+1 es domingo)

    Args:
        df: DataFrame con purchase_date y central_processing_date.

    Returns:
        df con la columna calc_timeliness (long; NULL si alguna fecha
        falta, 0 si mismo día).

    Ejemplo:
        calc_timeliness_draft(df)
    """
    df = df.withColumn("_central_date", F.to_date(F.col("central_processing_date")))
    df = df.withColumn("_purchase_date", F.to_date(F.col("purchase_date")))
    df = df.withColumn("_total_days", F.datediff(F.col("_central_date"), F.col("_purchase_date")))
    # dayofweek de purchase+1: Spark Sun=1..Sat=7; offset = (8 - dow) % 7
    df = df.withColumn("_start_dow", F.dayofweek(F.date_add(F.col("_purchase_date"), 1)))
    df = df.withColumn(
        "_sundays_count",
        F.greatest(
            F.lit(0),
            F.floor((F.col("_total_days") - 1 + 6 - (8 - F.col("_start_dow")) % 7) / 7)
        )
    )
    df = df.withColumn(
        "calc_timeliness",
        F.when(F.col("_total_days") == 0, F.lit(0))
         .when(F.col("_total_days").isNull() | F.col("_purchase_date").isNull() | F.col("_central_date").isNull(), F.lit(None))
         .otherwise(F.col("_total_days") - 1 - F.col("_sundays_count"))
         .cast(LongType())
    )
    df = df.drop("_central_date", "_purchase_date", "_total_days", "_start_dow", "_sundays_count")
    return df


def calc_timeliness_sms(df: DataFrame) -> DataFrame:
    """
    timeliness para SMS: settlement_date_sms - local_draft_date

    Args:
        df: DataFrame con settlement_date_sms y local_draft_date.

    Returns:
        df con la columna calc_timeliness (long, diferencia en días).

    Ejemplo:
        calc_timeliness_sms(df)
    """
    return df.withColumn(
        "calc_timeliness",
        F.datediff(F.to_date(F.col("settlement_date_sms")), F.to_date(F.col("local_draft_date"))).cast(LongType())
    )


# =============================================================================
# CAMPOS ESPECÍFICOS SMS
# =============================================================================
 
def calc_acquirer_bin(df: DataFrame) -> DataFrame:
    """
    Deriva acquirer_bin: primeros 6 caracteres de retrieval_reference_number.

    Args:
        df: DataFrame con la columna retrieval_reference_number.

    Returns:
        df con la columna calc_acquirer_bin agregada.
    """
    return df.withColumn("calc_acquirer_bin", F.substring(F.col("retrieval_reference_number"), 1, 6))
 
 
def calc_processing_code_transaction_type(df: DataFrame) -> DataFrame:
    """
    Deriva processing_code_transaction_type: primeros 2 caracteres de
    processing_code.

    Args:
        df: DataFrame con la columna processing_code.

    Returns:
        df con la columna calc_processing_code_transaction_type agregada.
    """
    return df.withColumn("calc_processing_code_transaction_type", F.substring(F.col("processing_code"), 1, 2))
 
 
def calc_source_amount_sms(df: DataFrame) -> DataFrame:
    """
    Copia la columna draft_amount a calc_source_amount.

    Args:
        df: DataFrame con la columna draft_amount.

    Returns:
        df con la columna calc_source_amount agregada.
    """
    return df.withColumn("calc_source_amount", F.col("draft_amount"))
 
 
def calc_transaction_code_sms(df: DataFrame) -> DataFrame:
    """
    transaction_code_sms basado en business_transaction_type + reversal_indicator.

    Clasifica primero el tipo de transacción (PUR/CRD/CSH) según
    calc_business_transaction_type, y luego mapea (tipo, reversal_indicator)
    al código transaction_code_sms de 2 dígitos correspondiente (05/06/07
    para no-reversal, 25/26/27 para reversal).

    Args:
        df: DataFrame con calc_business_transaction_type y
            calc_reversal_indicator.

    Returns:
        df con la columna calc_transaction_code_sms (código de 2 dígitos, o
        "" si no matchea ningún tipo).

    Ejemplo:
        calc_transaction_code_sms(df)
    """
    df = df.withColumn(
        "_transaction_type",
        F.when(F.col("calc_business_transaction_type").isin([1, 30, 27]), F.lit("PUR"))
         .when(F.col("calc_business_transaction_type").isin([3, 115]), F.lit("CRD"))
         .when(F.col("calc_business_transaction_type").isin([19, 20, 21, 22, 25]), F.lit("CSH"))
         .otherwise(F.lit(None))
    )
    
    df = df.withColumn(
        "calc_transaction_code_sms",
        F.when((F.col("_transaction_type") == "PUR") & (F.col("calc_reversal_indicator") == 0), F.lit("05"))
         .when((F.col("_transaction_type") == "CRD") & (F.col("calc_reversal_indicator") == 0), F.lit("06"))
         .when((F.col("_transaction_type") == "CSH") & (F.col("calc_reversal_indicator") == 0), F.lit("07"))
         .when((F.col("_transaction_type") == "PUR") & (F.col("calc_reversal_indicator") == 1), F.lit("25"))
         .when((F.col("_transaction_type") == "CRD") & (F.col("calc_reversal_indicator") == 1), F.lit("26"))
         .when((F.col("_transaction_type") == "CSH") & (F.col("calc_reversal_indicator") == 1), F.lit("27"))
         .otherwise(F.lit(""))
    )
    
    df = df.drop("_transaction_type")
    return df


# =============================================================================
# CAMPOS CALCULADOS VSS
# =============================================================================
 
def calc_vss_report_type(df: DataFrame, vss_type: str) -> DataFrame:
    """
    vss_report_type: int del vss_type (110, 120, 130, 140)

    Args:
        df: DataFrame de entrada (no se lee ninguna columna).
        vss_type: Tipo VSS como string, ej. "110".

    Returns:
        df con la columna calc_vss_report_type (long).

    Ejemplo:
        calc_vss_report_type(df, "110")
    """
    return df.withColumn("calc_vss_report_type", F.lit(int(vss_type)).cast(LongType()))
 
 
def calc_vss_aggregation_level(df: DataFrame, vss_type: str) -> DataFrame:
    """
    vss_aggregation_level — réplica exacta de la lógica de Standard 1.0.

    Valores posibles: 0 (hoja), 1 (nodo intermedio/padre), 10 (raíz).

    Lógica:
      - 10 : rollup_to == reporting_for  (nodo raíz, se reporta a sí mismo)
      -  1 : rollup_to != reporting_for  Y  reporting_for ∈ rollup_group
             (este nodo es él mismo un destino de rollup → nodo intermedio/padre)
      -  0 : rollup_to != reporting_for  Y  reporting_for ∉ rollup_group
             (nodo hoja, nadie le hace rollup)

    rollup_group = conjunto de todos los valores rollup_to donde rollup_to != reporting_for,
    es decir, el conjunto de identificadores que son destino de rollup de alguna otra fila.

    Nota: la implementación anterior navegaba la jerarquía hacia arriba en 3 iteraciones,
    lo que producía que todas las hojas recibieran nivel 2 en lugar de 0, porque el
    rollup_to de una hoja siempre pertenece a rollup_group por definición.

    Args:
        df: DataFrame VSS con las columnas
            rollup_to_sre_identifier_{vss_type} y
            reporting_for_sre_identifier_{vss_type}.
        vss_type: Tipo VSS como string, ej. "110".

    Returns:
        df con la columna calc_vss_aggregation_level (long; 0 si las
        columnas de rollup no existen para ese vss_type).

    Ejemplo:
        calc_vss_aggregation_level(df, "110")
    """
    rollup_col    = f"rollup_to_sre_identifier_{vss_type}"
    reporting_col = f"reporting_for_sre_identifier_{vss_type}"

    if rollup_col not in df.columns or reporting_col not in df.columns:
        log_info(f"  Warning: Missing rollup columns for VSS {vss_type}. Setting aggregation_level to 0.")
        return df.withColumn("calc_vss_aggregation_level", F.lit(0).cast(LongType()))

    # Conjunto de todos los rollup_to donde rollup_to != reporting_for.
    # Estos son los identificadores de nodos padre/intermedios (alguien les hace rollup).
    rollup_group_df = df.filter(
        F.col(rollup_col) != F.col(reporting_col)
    ).select(
        F.col(rollup_col).alias("_rollup_group_id")
    ).distinct()

    # LEFT JOIN: ¿es el reporting_for de esta fila un nodo padre?
    # Si el join encuentra match → _rollup_group_id no es null → nivel 1.
    df = df.join(
        F.broadcast(rollup_group_df),
        F.col(reporting_col) == F.col("_rollup_group_id"),
        how="left"
    )

    df = df.withColumn(
        "calc_vss_aggregation_level",
        F.when(
            F.col(rollup_col) == F.col(reporting_col),
            F.lit(10)                                           # raíz
        ).when(
            F.col("_rollup_group_id").isNotNull(),
            F.lit(1)                                            # nodo intermedio/padre
        ).otherwise(
            F.lit(0)                                            # hoja
        ).cast(LongType())
    ).drop("_rollup_group_id")

    return df


# =============================================================================
# FUNCIONES PRINCIPALES DE CÁLCULO
# =============================================================================
 
def calculate_baseii_fields(df: DataFrame, ardef: DataFrame, country_df: DataFrame,
                            currency_df: DataFrame, file_type: str, client_data: dict) -> DataFrame:
    """
    Calcula todos los campos adicionales para BASEII (30 campos).

    Orquesta el pipeline completo: join con ARDEF, campos directos del
    ARDEF, campos de coalesce/string manipulation, lógica condicional
    (business_mode, business_transaction_type/cycle, reversal_indicator),
    joins con country/currency, jurisdiction + jurisdiction_assigned +
    settlement_report_currency_code, timeliness, y selección final de las
    30 columnas de salida (con content_hash y record como primeras
    columnas — ver decisions.md → "Por qué se agrega content_hash...").

    Args:
        df: DataFrame CLN de BASEII.
        ardef: DataFrame ARDEF ya cargado (de load_visa_ardef).
        country_df: DataFrame de referencia country.
        currency_df: DataFrame de referencia currency.
        file_type: "IN" u "OUT".
        client_data: Dict de configuración del cliente (de get_client_data).

    Returns:
        DataFrame CAL con las 30 columnas de salida de BASEII.

    Ejemplo:
        calculate_baseii_fields(df, ardef, country_df, currency_df, "IN", client_data)
    """
    log_info("Calculating BASEII fields")
    log_info(f"Input records: {df.count():,}")
    
    # 1. JOIN con ARDEF
    log_info("  Joining with ARDEF...")
    df = join_with_ardef(df, ardef, "account_number")
    
    # 2. Campos directos del ARDEF
    log_info("  Calculating ARDEF-based fields...")
    df = calc_ardef_country(df)
    df = calc_b2b_program_id(df)
    df = calc_fast_funds(df)
    df = calc_funding_source(df)
    df = calc_issuer_country(df)
    df = calc_nnss_indicator(df)
    df = calc_product_id_ardef(df)
    df = calc_product_subtype(df)
    df = calc_technology_indicator(df)
    df = calc_travel_indicator(df)
    
    # 3. String manipulation
    log_info("  Calculating string/coalesce fields...")
    df = calc_issuer_bin_8(df, "account_number")
    df = calc_authorization_code_valid_draft(df)
    df = calc_business_application_id(df)
    df = calc_business_format_code(df)
    df = calc_message_reason_code(df)
    df = calc_network_identification_code(df)
    df = calc_type_of_purchase(df)
    df = calc_surcharge_amount(df)
    df = calc_token_requestor_id_draft(df)
    
    # 4. Lógica condicional
    log_info("  Calculating conditional fields...")
    df = calc_business_mode_draft(df, file_type)
    df = calc_business_transaction_type_draft(df, file_type)
    df = calc_business_transaction_cycle_draft(df)
    df = calc_reversal_indicator_draft(df)
    df = calc_jurisdiction_country_draft(df)
    
    # 5. JOINs adicionales
    log_info("  Calculating fields with reference table JOINs...")
    df = calc_issuer_region(df, country_df)
    df = calc_jurisdiction_region_draft(df, country_df)
    df = calc_source_currency_code_alphabetic_draft(df, currency_df)
    
    # 6. Jurisdiction (agrega merchant_region_code y ardef_region)
    log_info("  Calculating jurisdiction fields...")
    df = calc_jurisdiction_draft(df, country_df, file_type, client_data)
    df = calc_jurisdiction_assigned_draft(df)
    df = calc_settlement_report_currency_code_draft(df, client_data)

    # 7. Timeliness
    log_info("  Calculating timeliness...")
    df = calc_timeliness_draft(df)
    
    # 8. Seleccionar output
    log_info("  Selecting final output columns...")
    output_columns = [
        "content_hash",
        "record",
        F.col("calc_ardef_country").alias("ardef_country"),
        F.col("calc_authorization_code_valid").alias("authorization_code_valid"),
        F.col("calc_b2b_program_id").alias("b2b_program_id"),
        F.col("calc_business_application_id").alias("business_application_id"),
        F.col("calc_business_format_code").alias("business_format_code"),
        F.col("calc_business_mode").alias("business_mode"),
        F.col("calc_business_transaction_type").alias("business_transaction_type"),
        F.col("calc_business_transaction_cycle").alias("business_transaction_cycle"),
        F.col("calc_fast_funds").alias("fast_funds"),
        F.col("calc_funding_source").alias("funding_source"),
        F.col("calc_issuer_bin_8").alias("issuer_bin_8"),
        F.col("calc_issuer_country").alias("issuer_country"),
        F.col("calc_issuer_region").alias("issuer_region"),
        F.col("calc_jurisdiction").alias("jurisdiction"),
        F.col("calc_jurisdiction_assigned").alias("jurisdiction_assigned"),
        F.col("calc_jurisdiction_country").alias("jurisdiction_country"),
        F.col("calc_jurisdiction_region").alias("jurisdiction_region"),
        F.col("calc_message_reason_code").alias("message_reason_code"),
        F.col("calc_network_identification_code").alias("network_identification_code"),
        F.col("calc_nnss_indicator").alias("nnss_indicator"),
        F.col("calc_product_id").alias("product_id"),
        F.col("calc_product_subtype").alias("product_subtype"),
        F.col("calc_reversal_indicator").alias("reversal_indicator"),
        F.col("calc_settlement_report_currency_code").alias("settlement_report_currency_code"),
        F.col("calc_source_currency_code_alphabetic").alias("source_currency_code_alphabetic"),
        F.col("calc_surcharge_amount").alias("surcharge_amount"),
        F.col("calc_technology_indicator").alias("technology_indicator"),
        F.col("calc_timeliness").alias("timeliness"),
        F.col("calc_token_requestor_id").alias("token_requestor_id"),
        F.col("calc_travel_indicator").alias("travel_indicator"),
        F.col("calc_type_of_purchase").alias("type_of_purchase"),
    ]

    result = df.select(output_columns)
    log_info(f"BASEII calculation complete. Output columns: {len(output_columns)}")
    return result


def calculate_sms_fields(df: DataFrame, ardef: DataFrame, country_df: DataFrame,
                         currency_df: DataFrame, client_data: dict) -> DataFrame:
    """
    Calcula todos los campos adicionales para SMS (27 campos).

    Mismo patrón que calculate_baseii_fields, con las variantes _sms de cada
    función (join con ARDEF por card_number en vez de account_number, además
    de los campos específicos de SMS: acquirer_bin,
    processing_code_transaction_type, source_amount, transaction_code_sms).

    Args:
        df: DataFrame CLN de SMS.
        ardef: DataFrame ARDEF ya cargado (de load_visa_ardef).
        country_df: DataFrame de referencia country.
        currency_df: DataFrame de referencia currency.
        client_data: Dict de configuración del cliente (de get_client_data).

    Returns:
        DataFrame CAL con las 27 columnas de salida de SMS.

    Ejemplo:
        calculate_sms_fields(df, ardef, country_df, currency_df, client_data)
    """
    log_info("Calculating SMS fields")
    log_info(f"Input records: {df.count():,}")
    
    # 1. JOIN con ARDEF usando card_number
    log_info("  Joining with ARDEF...")
    df = join_with_ardef(df, ardef, "card_number")
    
    # 2. Campos directos del ARDEF
    log_info("  Calculating ARDEF-based fields...")
    df = calc_ardef_country(df)
    df = calc_b2b_program_id(df)
    df = calc_fast_funds(df)
    df = calc_funding_source(df)
    df = calc_issuer_country(df)
    df = calc_nnss_indicator(df)
    df = calc_product_id_ardef(df)
    df = calc_product_subtype(df)
    df = calc_technology_indicator(df)
    df = calc_travel_indicator(df)
    
    # 3. Campos específicos SMS
    log_info("  Calculating SMS-specific fields...")
    df = calc_acquirer_bin(df)
    df = calc_issuer_bin_8(df, "card_number")
    df = calc_authorization_code_valid_sms(df)
    df = calc_processing_code_transaction_type(df)
    df = calc_source_amount_sms(df)
    
    # 4. Lógica condicional
    log_info("  Calculating conditional fields...")
    df = calc_business_mode_sms(df)
    df = calc_business_transaction_type_sms(df)
    df = calc_business_transaction_cycle_sms(df)
    df = calc_reversal_indicator_sms(df)
    df = calc_jurisdiction_country_sms(df)
    
    # 5. JOINs adicionales
    log_info("  Calculating fields with reference table JOINs...")
    df = calc_issuer_region(df, country_df)
    df = calc_jurisdiction_region_sms(df, country_df)
    df = calc_source_currency_code_alphabetic_sms(df, currency_df)
    
    # 6. Jurisdiction (agrega merchant_country_code, merchant_region_code, ardef_region)
    log_info("  Calculating jurisdiction fields...")
    df = calc_jurisdiction_sms(df, country_df, client_data)
    df = calc_jurisdiction_assigned_sms(df)
    
    # 7. Timeliness y transaction_code
    log_info("  Calculating timeliness and transaction_code...")
    df = calc_timeliness_sms(df)
    df = calc_transaction_code_sms(df)
    
    # 8. Seleccionar output
    log_info("  Selecting final output columns...")
    output_columns = [
        "content_hash",
        "record",
        F.col("calc_acquirer_bin").alias("acquirer_bin"),
        F.col("calc_ardef_country").alias("ardef_country"),
        F.col("calc_authorization_code_valid").alias("authorization_code_valid"),
        F.col("calc_b2b_program_id").alias("b2b_program_id"),
        F.col("calc_business_mode").alias("business_mode"),
        F.col("calc_business_transaction_type").alias("business_transaction_type"),
        F.col("calc_business_transaction_cycle").alias("business_transaction_cycle"),
        F.col("calc_fast_funds").alias("fast_funds"),
        F.col("calc_funding_source").alias("funding_source"),
        F.col("calc_issuer_bin_8").alias("issuer_bin_8"),
        F.col("calc_issuer_country").alias("issuer_country"),
        F.col("calc_issuer_region").alias("issuer_region"),
        F.col("calc_jurisdiction").alias("jurisdiction"),
        F.col("calc_jurisdiction_assigned").alias("jurisdiction_assigned"),
        F.col("calc_jurisdiction_country").alias("jurisdiction_country"),
        F.col("calc_jurisdiction_region").alias("jurisdiction_region"),
        F.col("calc_nnss_indicator").alias("nnss_indicator"),
        F.col("calc_processing_code_transaction_type").alias("processing_code_transaction_type"),
        F.col("calc_product_id").alias("product_id"),
        F.col("calc_product_subtype").alias("product_subtype"),
        F.col("calc_reversal_indicator").alias("reversal_indicator"),
        F.col("calc_source_amount").alias("source_amount"),
        F.col("calc_source_currency_code_alphabetic").alias("source_currency_code_alphabetic"),
        F.col("calc_technology_indicator").alias("technology_indicator"),
        F.col("calc_timeliness").alias("timeliness"),
        F.col("calc_transaction_code_sms").alias("transaction_code_sms"),
        F.col("calc_travel_indicator").alias("travel_indicator"),
    ]
    
    result = df.select(output_columns)
    log_info(f"SMS calculation complete. Output columns: {len(output_columns)}")
    return result


def calculate_vss_fields(df: DataFrame, vss_type: str) -> DataFrame:
    """
    Calcula los campos para VSS (2 campos).

    Args:
        df: DataFrame CLN de un tipo VSS (110/120/130/140).
        vss_type: Tipo VSS como string, ej. "110".

    Returns:
        DataFrame CAL con content_hash, record, vss_report_type,
        vss_aggregation_level.

    Ejemplo:
        calculate_vss_fields(df, "110")
    """
    log_info(f"Calculating VSS {vss_type} fields")
    log_info(f"Input records: {df.count():,}")
    
    df = calc_vss_report_type(df, vss_type)
    df = calc_vss_aggregation_level(df, vss_type)
    
    output_columns = [
        "content_hash",
        "record",
        F.col("calc_vss_report_type").alias("vss_report_type"),
        F.col("calc_vss_aggregation_level").alias("vss_aggregation_level"),
    ]
    
    result = df.select(output_columns)
    log_info(f"VSS {vss_type} calculation complete. Output columns: {len(output_columns)}")
    return result


# =============================================================================
# HELPER: PROCESAR OUTPUT
# =============================================================================
 
def process_output(output_config: dict, staging_bucket: str, reference_bucket: str,
                   file_type: str, file_date: str, client_data: dict, ardef: DataFrame,
                   country_df: DataFrame, currency_df: DataFrame) -> dict:
    """
    Procesa un output específico (BASEII, SMS, o VSS_xxx) de punta a punta:
    carga el CLN, despacha a la función de cálculo correspondiente
    (calculate_baseii_fields/calculate_sms_fields/calculate_vss_fields), y
    escribe el resultado al path CAL derivado (reemplazando "/300_"→"/400_" y
    "_cln"→"_cal" en el s3_key de entrada).

    SIN try/except - errores suben y matan el job (Step Functions verá rojo).

    Args:
        output_config: Dict de un output del CLN, con output_type y s3_key.
        staging_bucket: Bucket S3 de staging.
        reference_bucket: Bucket S3 de referencia (no usado directamente acá,
            ya se cargaron los DataFrames de referencia antes de llamar a
            esta función).
        file_type: "IN" u "OUT".
        file_date: Fecha del archivo (no usado directamente, solo logging).
        client_data: Dict de configuración del cliente.
        ardef: DataFrame ARDEF ya cargado.
        country_df: DataFrame de referencia country.
        currency_df: DataFrame de referencia currency.

    Returns:
        Dict con status="SUCCESS", output_type, s3_key (de salida), records.

    Raises:
        ValueError: si el output_config no trae s3_key, o si output_type no
            es BASEII, SMS, ni empieza con "VSS_".

    Ejemplo:
        process_output({'output_type': 'BASEII', 's3_key': '.../300_baseii_cln_drafts/.../x.parquet'},
                        staging_bucket, reference_bucket, "IN", "2026-01-03", client_data,
                        ardef, country_df, currency_df)
    """
    output_type = output_config.get('output_type', '')
    input_s3_key = output_config.get('s3_key', '')
    
    if not input_s3_key:
        raise ValueError(f"No s3_key in output_config for {output_type}")
    
    input_path = f"s3://{staging_bucket}/{input_s3_key}"
    output_s3_key = input_s3_key.replace('/300_', '/400_').replace('_cln', '_cal')
    output_path = f"s3://{staging_bucket}/{output_s3_key}"
    
    log_info(f"Processing {output_type}")
    log_info(f"  Input:  {input_path}")
    log_info(f"  Output: {output_path}")
    
    df = load_parquet_safe(input_path)
    
    if output_type == 'BASEII':
        result_df = calculate_baseii_fields(df, ardef, country_df, currency_df, file_type, client_data)
    elif output_type == 'SMS':
        result_df = calculate_sms_fields(df, ardef, country_df, currency_df, client_data)
    elif output_type.startswith('VSS_'):
        vss_type = output_type.replace('VSS_', '')
        result_df = calculate_vss_fields(df, vss_type)
    else:
        raise ValueError(f"Unknown output_type: {output_type}")
    
    result_df = result_df.cache()
    record_count = result_df.count()
    save_parquet(result_df, output_path)
    result_df.unpersist()
    
    return {
        'status': 'SUCCESS',
        'output_type': output_type,
        's3_key': output_s3_key,
        'records': record_count
    }


# =============================================================================
# MAIN
# =============================================================================
 
def main():
    """
    Punto de entrada del Glue job glue-vi-calculate. Resuelve los argumentos
    del job, carga los datos del cliente (DynamoDB) y las tablas de
    referencia (ARDEF filtrado por file_date, country, currency), y procesa
    cada output del archivo (process_output) — uno por tipo de record
    (BASEII/SMS/VSS_xxx) presente en el archivo.

    Returns:
        Dict con status="SUCCESS", total_outputs, total_records, outputs
        (lista de resultados de process_output). Llama a job.commit() al
        finalizar.

    Ejemplo:
        main()  # invocado automáticamente al ejecutar el script como Glue job
    """
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'reference_bucket', 
        'staging_bucket', 
        'client_id', 
        'file_id', 
        'file_type', 
        'file_date',
        'outputs', 
        'dynamodb_table_client'
    ])
    
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    client_id = args['client_id']
    file_id = args['file_id']
    file_type = args['file_type']
    file_date = args['file_date']
    staging_bucket = args['staging_bucket']
    reference_bucket = args['reference_bucket']
    dynamodb_table_client = args['dynamodb_table_client']
    outputs = json.loads(args['outputs'])
    
    log_info("=" * 70)
    log_info("ITX-CALCULATE (PySpark) - STARTING")
    log_info("=" * 70)
    log_info(f"Client ID:        {client_id}")
    log_info(f"File ID:          {file_id}")
    log_info(f"File Type:        {file_type}")
    log_info(f"File Date:        {file_date}")
    log_info(f"Staging Bucket:   {staging_bucket}")
    log_info(f"Reference Bucket: {reference_bucket}")
    log_info(f"Outputs:          {len(outputs)}")
    log_info(f"DynamoDB client table: {dynamodb_table_client}")
    log_info("=" * 70)
    
    # Cargar datos del cliente
    log_info(f"Loading client data: {client_id}")
    client_data = get_client_data(client_id, dynamodb_table_client)
    
    # Convertir file_date
    try:
        file_date_obj = datetime.strptime(file_date, "%Y-%m-%d").date()
    except ValueError:
        file_date_obj = date.today()
    
    # Cargar tablas de referencia
    log_info("Loading reference tables...")
    ardef = load_visa_ardef(reference_bucket, file_date_obj)
    country_df = load_country_table(reference_bucket).cache()
    currency_df = load_currency_table(reference_bucket).cache()
    
    # Procesar cada output
    results = []
    total_records = 0
    
    for output_config in outputs:
        output_type = output_config.get('output_type', 'UNKNOWN')
        log_info("")
        log_info("=" * 60)
        log_info(f"Processing: {output_type}")
        log_info("=" * 60)
        
        result = process_output(
            output_config=output_config,
            staging_bucket=staging_bucket,
            reference_bucket=reference_bucket,
            file_type=file_type,
            file_date=file_date,
            client_data=client_data,
            ardef=ardef,
            country_df=country_df,
            currency_df=currency_df
        )
        
        results.append(result)
        total_records += result.get('records', 0)
        log_info(f"  ✓ {output_type}: {result.get('records', 0):,} records")
    
    log_info("")
    log_info("=" * 70)
    log_info("CALCULATE PROCESS COMPLETED")
    log_info("=" * 70)
    log_info(f"Total outputs: {len(results)}")
    log_info(f"Total records: {total_records:,}")
    
    output_data = {
        'status': 'SUCCESS',
        'total_outputs': len(results),
        'total_records': total_records,
        'outputs': results
    }
    
    log_info(f"Output: {json.dumps(output_data)}")
 
    ardef.unpersist()
    country_df.unpersist()
    currency_df.unpersist()
 
    job.commit()
    
    return output_data


if __name__ == "__main__":
    main()