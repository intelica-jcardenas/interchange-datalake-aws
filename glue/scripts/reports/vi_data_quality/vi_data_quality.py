"""
vi_data_quality.py — Job real: itl-0004-itx-dev-intchg-02-glue-vi-data-quality
================================================================================
Archivo:     glue/scripts/reports/vi_data_quality/vi_data_quality.py
S3 Script:   s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/vi_data_quality.py

Data Quality – VISA BaseII (Transaccional). Etapa 1 de 2 – Solo parte
transaccional (BaseII).
Pendiente: Etapa 2 – Parte de liquidación (VSS 130), pendiente de validación.
Aún no integrado a ningún Step Function (ver CLAUDE.md → tabla de Glue
Jobs) — smoke test OK (2026-07-08), sin ejecución en producción.

Replica en PySpark (AWS Glue 4.0) la lógica del Standard 1.0 implementada
en la función PostgreSQL get_visa_validation_results_baseii().

Lee los parquets BaseII del bucket Operational (que ya contienen datos
fusionados de las etapas CLN + CAL + ITX), aplica filtros de condiciones
de validación y tablas de referencia, y genera un parquet de resumen
agrupado con métricas de calidad de datos transaccional VISA.

Equivalencia Standard 1.0 → Standard 2.0:
  dh_visa_transaction                     → operational/{client}/VISA/baseii_drafts/
  dh_visa_transaction_calculated_field    → mismo parquet BaseII (campos _cln embebidos)
  dh_visa_interchange (T3)                → mismo parquet BaseII (campos _itx embebidos)
  T3.calculated_value                     → interchange_fee_amount_itx
  T3.fee_currency                         → interchange_fee_currency
  M3.fee_descriptor (m_interchange_rules) → interchange_fee_descriptor (pre-calculado)
  m_visa_business_transaction_type (M1)   → reference/visa_business_transaction_type/
  m_visa_business_transaction_cycle (M2)  → reference/visa_business_transaction_cycle/
  dh_exchange_rate (X1, X2)               → reference/exchange-rates-glue/ (Hive: brand=/exchange_date=)
  t_customer                              → DynamoDB: itl-0004-itx-dev-dynamo-client-02
  t_control_file (SBSA hash filter)       → DynamoDB: itl-0004-itx-dev-dynamo-file_control-02
  validation.validation_conditions        → reference/validation_conditions/data.parquet

Job Parameters:
  --client_id                   Un cliente o varios separados por coma: "EBGR" | "EBGR,SBSA"
  --issuer_acquirer_indicator   Un modo o ambos separados por coma: "A" | "I" | "A,I"
                                   · A → Acquirer (file_type=OUT)
                                   · I → Issuer   (file_type=IN)
  --start_date                  YYYY-MM-DD (inicio del rango a procesar)
  --end_date                    YYYY-MM-DD (fin del rango a procesar)
  --operational_bucket          itl-0004-itx-dev-intchg-02-s3-operational
  --reference_bucket            itl-0004-itx-dev-intchg-02-s3-reference
  --analytics_bucket            itl-0004-itx-dev-intchg-02-s3-analytics
  --dynamodb_table_client       itl-0004-itx-dev-dynamo-client-02
  --dynamodb_table_file_control itl-0004-itx-dev-dynamo-file_control-02
  --brand_local_override        (OPCIONAL) Fuerza un único valor de brand_local para
                                   todos los clientes del run. Solo se respeta si el
                                   cliente tiene 'hash_file_filter' configurado en
                                   validation_conditions. Si el cliente no lo tiene,
                                   el override se ignora con un WARNING en el log y se
                                   usa el comportamiento estándar ('default').
                                   Valores válidos: brand | local
                                   Ejemplo de uso manual/selectivo para SBSA:
                                     solo brand → --brand_local_override brand
                                     solo local → --brand_local_override local
                                   Si NO se pasa este parámetro, los valores a iterar
                                   se leen desde validation_conditions
                                   (vc_condition_type='brand_local_values').
                                   Clientes sin fila configurada usan ['default'].

Salida:
  Un Parquet por client_id:
  s3://{analytics_bucket}/{client_id}/reports/tst_{client_id}_data_quality.parquet
  Columnas: app_processing_date, data_source, file_source, app_customer_code,
            business_mode, jurisdiction, settlement_currency, reversal_indicator,
            trx_type, trx_cycle, fee_descriptor, report_currency_code,
            trx_count, trx_amt, itx_amt

Notas importantes:
  · SMS (get_visa_validation_results_sms): NO aplica. Confirmado que no se
    invoca en el store procedure de Standard 1.0 y no existe en Standard 2.0.
  · iqa_setcur: NO aplica para VI/BaseII/validation. Siempre se usa la lógica
    default (settlement_report_currency_code vs local_currency_code).
  · WHERE conditions (base_ii_restriction): la tabla validation_conditions no
    contiene condiciones WHERE para VI/BaseII/validation actualmente. Se lee y
    loguea por extensibilidad, pero no se aplica ningún filtro adicional.
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import re
from pyspark.sql.types import IntegerType, DoubleType, StringType, LongType, DecimalType
import boto3
from boto3.dynamodb.conditions import Attr


# =============================================================================
# CONFIGURACIÓN SPARK
# =============================================================================

spark = SparkSession.builder \
    .config("spark.sql.parquet.int96RebaseModeInRead",    "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInWrite",   "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite","CORRECTED") \
    .config("spark.sql.parquet.outputTimestampType",       "TIMESTAMP_MICROS") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
logger      = glueContext.get_logger()


def log_info(message: str):
    logger.info(f"GlueLogger: {message}")


def log_error(message: str):
    logger.error(f"GlueLogger: {message}")


# =============================================================================
# HELPERS: S3 / PARQUET
# =============================================================================

def save_parquet(df: DataFrame, path: str):
    """
    Guarda el DataFrame como Parquet en S3.

    Al ser un reporte de Data Quality (volumen reducido de filas agrupadas),
    se usa coalesce(1) para producir un único archivo Parquet fácil de abrir
    y explorar manualmente (p. ej. desde Athena o un cliente local).

    Si el volumen crece significativamente en el futuro, cambiar a repartition(N).
    """
    count = df.count()
    log_info(f"  Saving {count:,} rows → {path}")
    df.coalesce(1).write.mode("overwrite").parquet(path)
    log_info(f"  ✓ Saved: {path}")


# =============================================================================
# HELPERS: DYNAMODB – MAESTRA DE CLIENTES
# =============================================================================

def get_client_data(client_id: str, dynamodb_table_client: str) -> dict:
    """
    Obtiene los metadatos del cliente desde la tabla DynamoDB m_client.
    Equivale a la tabla control.t_customer (C1) de Standard 1.0.

    Campos críticos para el cálculo:
      · local_currency_code  → determina si la transacción es 'Local' o 'Foreign'
                                comparando con settlement_report_currency_code del parquet
      · report_currency_code → moneda destino a la que se convierten los importes
                                del output (trx_amt e itx_amt)
    """
    log_info(f"Loading client data from DynamoDB [{dynamodb_table_client}] for: {client_id}")
    dynamodb = boto3.resource("dynamodb")
    table    = dynamodb.Table(dynamodb_table_client)
    response = table.get_item(Key={"client_id": client_id})

    if "Item" not in response:
        raise ValueError(f"Client not found in DynamoDB: {client_id}")

    item = response["Item"]
    log_info(f"  Client: {item.get('client_name', client_id)} | "
             f"local_currency={item.get('local_currency_code')} | "
             f"report_currency={item.get('report_currency_code')}")

    return {
        "client_id":              client_id,
        "client_name":            item.get("client_name",            ""),
        "local_currency_code":    item.get("local_currency_code",    ""),
        "report_currency_code":   item.get("report_currency_code",   ""),
        "settlement_currency_code": item.get("settlement_currency_code", ""),
    }


# =============================================================================
# HELPERS: DYNAMODB – FILE CONTROL (FILTRO DE HASH)
# =============================================================================

def check_hash_filter_needed(
    conditions_df: DataFrame,
    client_id: str,
    data_source: str,
    business_mode_code: str
) -> bool:
    """
    Determina si este cliente necesita filtro de hash para la fuente de datos
    indicada, leyendo la bandera 'hash_file_filter' de validation_conditions en S3.

    En Standard 1.0, la función baseii consultaba validation_conditions así:
        SELECT vc_query_code INTO hash_file_filter
        FROM validation.validation_conditions
        WHERE vc_report='validation' AND vc_brand='VI'
          AND vc_customer_code = customer_code
          AND vc_condition_type = 'hash_file_filter'
          AND vc_data_source = data_source
          AND vc_business_mode = business_mode

    Si encontraba una fila, activaba el filtro; si no, lo omitía.

    En Standard 2.0, esta función lee esa misma bandera de forma GENÉRICA.
    Esto permite que en el futuro cualquier cliente pueda activar el hash_filter
    añadiendo una fila en validation_conditions sin tocar el código.

    Datos actuales (validation + VI + base_ii):
      · SBSA / mode='1' (Acquirer) → necesita filtro → retorna True
      · Otros clientes / Issuer    → sin fila → retorna False

    Parámetros:
      · data_source       : 'base_ii' (Etapa 1) o 'vss' (Etapa 2)
      · business_mode_code: '1' (Acquirer) o '2' (Issuer)
    """
    count = conditions_df.filter(
        (F.col("vc_report")         == "validation") &
        (F.col("vc_brand")          == "VI") &
        (F.col("vc_customer_code")  == client_id) &
        (F.col("vc_condition_type") == "hash_file_filter") &
        (F.col("vc_data_source")    == data_source) &
        (F.col("vc_business_mode").cast(StringType()) == business_mode_code)
    ).count()

    needs_filter = count > 0
    log_info(f"  hash_file_filter check → client={client_id} / "
             f"source={data_source} / mode={business_mode_code} → needs_filter={needs_filter}")
    return needs_filter


def get_brand_local_values(
    conditions_df: DataFrame,
    client_id: str,
    data_source: str,
    business_mode_code: str,
    override: str = None
) -> list:
    """
    Determina los valores de brand_local_indicator a iterar para este cliente,
    data_source y business_mode.

    Hay dos modos de operación:

    1. SIN override (ejecución estándar — comportamiento normal):
       Lee vc_condition_type='brand_local_values' de validation_conditions y retorna
       la lista de valores configurados. Si no hay fila, retorna ['default'].
       Equivale al loop FOREACH brand_local_ind del SP de Standard 1.0.

    2. CON override (ejecución selectiva manual):
       Permite forzar un único valor de brand_local para un run puntual sin modificar
       la configuración de validation_conditions (ej. "hoy solo quiero local").

       GUARDIA: el override solo se respeta si el cliente tiene 'hash_file_filter'
       configurado en validation_conditions para este data_source y business_mode.
       Razón: brand_local ('brand'/'local') solo tiene sentido para clientes que
       usan filtro de hash para distinguir archivos — actualmente solo SBSA.
       Si el cliente NO tiene hash_file_filter (ej. EBGR), el override se ignora
       con un WARNING y se usa el comportamiento estándar, evitando que se etiquete
       el campo file_source con un valor incorrecto ('brand'/'local') en el output.

    Datos actuales en validation_conditions:
      · SBSA / base_ii / mode=1 → brand_local_values='brand,local' + hash_file_filter
      · SBSA / vss     / mode=1 → brand_local_values='brand,local' + hash_file_filter
      · Resto de clientes       → sin filas → ['default']
    """

    # ── Caso con override: aplicar validación de guardia antes de usarlo ──────
    if override:
        # Verificar si este cliente tiene 'hash_file_filter' configurado en
        # validation_conditions para este data_source y business_mode.
        # Solo los clientes con esta configuración distinguen archivos por
        # brand_local — para los demás no tiene sentido aplicar el override.
        tiene_hash_filter = conditions_df.filter(
            (F.col("vc_report")         == "validation") &
            (F.col("vc_brand")          == "VI") &
            (F.col("vc_customer_code")  == client_id) &
            (F.col("vc_condition_type") == "hash_file_filter") &
            (F.col("vc_data_source")    == data_source) &
            (F.col("vc_business_mode").cast(StringType()) == business_mode_code)
        ).count() > 0

        if tiene_hash_filter:
            # El cliente usa filtro de hash → el override es válido, se aplica
            log_info(f"  [OVERRIDE] brand_local_override='{override}' aceptado para "
                     f"{client_id}/{data_source}/mode={business_mode_code} "
                     f"(cliente tiene hash_file_filter configurado) → ['{override}']")
            return [override]
        else:
            # El cliente NO usa filtro de hash → el override no tiene efecto útil
            # para este cliente. Se ignora para evitar un file_source incorrecto
            # en el output (ej. EBGR con override='local' quedaría mal etiquetado).
            log_info(f"  [WARNING] brand_local_override='{override}' IGNORADO para "
                     f"{client_id}/{data_source}/mode={business_mode_code}: "
                     f"el cliente no tiene 'hash_file_filter' en validation_conditions. "
                     f"El override de brand_local solo aplica a clientes con filtro de "
                     f"hash (actualmente: SBSA). Usando comportamiento estándar.")
            # Continúa hacia el flujo estándar de abajo

    # ── Caso estándar: leer valores configurados en validation_conditions ──────
    rows = conditions_df.filter(
        (F.col("vc_report")         == "validation") &
        (F.col("vc_brand")          == "VI") &
        (F.col("vc_customer_code")  == client_id) &
        (F.col("vc_condition_type") == "brand_local_values") &
        (F.col("vc_data_source")    == data_source) &
        (F.col("vc_business_mode").cast(StringType()) == business_mode_code)
    ).collect()

    if rows:
        # Hay configuración para este cliente/modo → parsear lista separada por coma
        # Ej: 'brand,local' → ['brand', 'local']
        values = [v.strip() for v in rows[0]["vc_query_code"].split(",") if v.strip()]
        log_info(f"  brand_local_values para {client_id}/{data_source}/mode={business_mode_code}: {values}")
        return values

    # Sin configuración y sin override válido → comportamiento por defecto
    log_info(f"  No hay brand_local_values configurados para "
             f"{client_id}/{data_source}/mode={business_mode_code} → ['default']")
    return ["default"]


def get_valid_content_hashes(
    client_id: str,
    brand_local_indicator: str,
    start_date: str,
    end_date: str,
    dynamodb_table_file_control: str
) -> list:
    """
    Obtiene los content_hash válidos para el cliente desde file_control en DynamoDB.

    Contexto de negocio (Standard 1.0):
    ─────────────────────────────────────
    Cuando se detectaba una fila 'hash_file_filter' en validation_conditions,
    Standard 1.0 consultaba t_control_file filtrando por process_file_name para
    obtener los hashes (app_hash_file) válidos a incluir:
        AND app_hash_file IN ( 'hash1', 'hash2', ... )

    Según el valor de brand_local_indicator:
      · 'brand'   → LIKE '%VISA_Outward%' OR LIKE '%VISA_Inward%'
      · 'local'   → LIKE '%Local_VISA_%'
      · 'default' → sin filtro adicional (esta función NO se llama)

    En Standard 2.0:
    ─────────────────
    t_control_file → file_control en DynamoDB
    process_file_name → landing_file_name
    app_hash_file     → content_hash

    Scan paginado de DynamoDB (máx. 1MB/llamada). Filtro server-side por
    client_id, brand_id y fechas. Filtro de landing_file_name en Python.

    Función GENÉRICA: se llama para cualquier cliente con hash_file_filter
    configurado, no solo para SBSA.
    """
    log_info(f"[HASH_FILTER] Scanning file_control in DynamoDB [{dynamodb_table_file_control}]")
    log_info(f"  client={client_id} | brand_local_indicator={brand_local_indicator} | "
             f"range: {start_date} → {end_date}")

    dynamodb = boto3.resource("dynamodb")
    table    = dynamodb.Table(dynamodb_table_file_control)

    # ── Filtro base en DynamoDB (server-side) ─────────────────────────────────
    # Nota: DynamoDB no soporta LIKE, por lo que el filtro de landing_file_name
    # se aplica en Python tras obtener los items del scan.
    filter_expr = (
        Attr("client_id").eq(client_id) &
        Attr("brand_id").eq("VI") &
        Attr("file_processing_date").between(start_date, end_date)
    )

    # ── Scan paginado (DynamoDB retorna máx. 1MB / llamada) ───────────────────
    valid_hashes  = []
    scan_kwargs   = {"FilterExpression": filter_expr}

    while True:
        response = table.scan(**scan_kwargs)
        items    = response.get("Items", [])

        for item in items:
            landing_file_name = item.get("landing_file_name", "")
            content_hash      = item.get("content_hash",      "")

            if not content_hash:
                continue

            # ── Equivalente al LIKE de Standard 1.0 ──────────────────────────
            if brand_local_indicator.lower() == "brand":
                # AND (process_file_name LIKE '%VISA_Outward%' OR LIKE '%VISA_Inward%')
                if "VISA_Outward" in landing_file_name or "VISA_Inward" in landing_file_name:
                    valid_hashes.append(content_hash)
            elif brand_local_indicator.lower() == "local":
                # AND process_file_name LIKE '%Local_VISA_%'
                if "Local_VISA_" in landing_file_name:
                    valid_hashes.append(content_hash)

        # Continuar paginando si hay más resultados
        if "LastEvaluatedKey" in response:
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        else:
            break

    log_info(f"  [HASH_FILTER] Found {len(valid_hashes):,} valid content_hashes for {client_id}")
    return valid_hashes


# =============================================================================
# HELPERS: REFERENCIA S3 – CONDICIONES DE VALIDACIÓN
# =============================================================================

def load_validation_conditions(reference_bucket: str) -> DataFrame:
    """
    Carga la tabla de condiciones de validación desde S3.
    En Standard 1.0 equivale a validation.validation_conditions en PostgreSQL.

    Esta tabla controla comportamientos específicos por reporte/marca/cliente/modo:
      · vc_condition_type = 'where'              → SQL WHERE extra por cliente
      · vc_condition_type = 'hash_file_filter'   → filtro de hashes para SBSA
      · vc_condition_type = 'iqa_setcur'         → override de lógica settlement_currency
      · vc_condition_type = 'brand_local_values' → lista de valores brand_local a iterar
                                                   por cliente/modo (reemplaza el param
                                                   externo --brand_local_indicator)

    Para VI/BaseII/validation:
      · 'iqa_setcur'  → NO existe ninguna fila configurada. Se usa siempre el default.
      · 'where'       → NO existe ninguna fila para vc_report='validation'. La función
                        apply_where_condition() logueará si se encuentra alguna en el futuro.
      · 'hash_file_filter' → Bandera genérica que indica qué clientes necesitan
                             filtro de hash. Actualmente: SBSA/base_ii/mode=1.
                             Se consulta dinámicamente con check_hash_filter_needed().
    """
    path = f"s3://{reference_bucket}/validation_conditions/data.parquet"
    log_info(f"Loading validation_conditions from: {path}")
    df    = spark.read.parquet(path)
    count = df.count()
    log_info(f"  Loaded {count:,} condition rows")
    return df


def apply_where_condition(
    df: DataFrame,
    conditions_df: DataFrame,
    client_id: str,
    data_source: str,
    business_mode_code: str
) -> DataFrame:
    """
    Aplica condiciones WHERE adicionales desde validation_conditions al DataFrame.
    Equivale a los bloques 'base_ii_restriction' / 'vss_restriction' de Standard 1.0.

    Comportamiento:
    ───────────────
    Para VI/BaseII/vc_report='validation': NINGÚN cliente tiene condición WHERE
    configurada → retorna el DataFrame sin modificar.

    Para VI/VSS/vc_report='validation': NGGR tiene la condición:
        "and t1.rollup_to_sre_identifier_130 in ('1000737016','1000737015')"
    Esta función la parsea con regex y aplica el filtro en PySpark.

    Patrón de vc_query_code soportado:
        [alias.]campo IN ('v1','v2',...)
    Para patrones no reconocidos → log WARNING + retorna df sin modificar.
    El patrón hash_file_filter (AND app_hash_file IN (/*replace*/)) NUNCA se
    parsea aquí — los hashes se obtienen de DynamoDB y se aplican con apply_hash_filter().
    """
    rows = conditions_df.filter(
        (F.col("vc_report")         == "validation") &
        (F.col("vc_brand")          == "VI") &
        (F.col("vc_customer_code")  == client_id) &
        (F.col("vc_condition_type") == "where") &
        (F.col("vc_data_source")    == data_source) &
        (F.col("vc_business_mode").cast(StringType()) == business_mode_code)
    ).collect()

    if not rows:
        log_info(f"  No hay condición WHERE para {client_id}/{data_source}/mode={business_mode_code} → sin filtro adicional")
        return df

    vc_query_code = rows[0]["vc_query_code"]
    log_info(f"  Condición WHERE encontrada ({client_id}/{data_source}/mode={business_mode_code}): {vc_query_code}")

    # Parsear patrón: [alias.]campo IN ('v1','v2',...)
    patron = r"[\w.]*?(\w+)\s+in\s+\(([^)]+)\)"
    match  = re.search(patron, vc_query_code, re.IGNORECASE)

    if match:
        campo   = match.group(1)
        valores = re.findall(r"'([^']+)'", match.group(2))
        log_info(f"  Aplicando filtro PySpark: {campo} IN {valores}")
        return df.filter(F.col(campo).isin(valores))
    else:
        log_info(f"  [WARNING] Patrón WHERE no reconocido — se esperaba 'campo IN (...)'. "
                 f"Condición ignorada: {vc_query_code}")
        return df


# =============================================================================
# HELPERS: REFERENCIA S3 – TABLAS MAESTRAS
# =============================================================================

def load_business_transaction_type(reference_bucket: str) -> DataFrame:
    """
    Carga la maestra de tipos de transacción de negocio VISA.
    En Standard 1.0: operational.m_visa_business_transaction_type (alias M1)

    Uso en el cálculo:
      JOIN key (BaseII): baseii.business_transaction_type (Integer) = M1.business_transaction_type_id
      JOIN key (VSS)   : vss.business_draft_type_130 (String)       = M1.business_transaction_type_code
      Campo out: M1.short_description → columna 'trx_type' en el output
                 (ej. "Purchase", "Refund", "Cash Advance")

    Se carga completa y se hace broadcast porque es una tabla pequeña de catálogo.
    """
    path = f"s3://{reference_bucket}/visa_business_transaction_type/data.parquet"
    log_info(f"Loading visa_business_transaction_type (M1) from: {path}")
    df = spark.read.parquet(path).select(
        F.col("business_transaction_type_id").cast(IntegerType()).alias("btt_id"),
        F.col("business_transaction_type_code").alias("btt_code"),
        F.col("short_description").alias("trx_type")
    )
    count = df.count()
    log_info(f"  Loaded {count:,} transaction types")
    return df


def load_business_transaction_cycle(reference_bucket: str) -> DataFrame:
    """
    Carga la maestra de ciclos de transacción de negocio VISA.
    En Standard 1.0: operational.m_visa_business_transaction_cycle (alias M2)

    Uso en el cálculo:
      JOIN key (BaseII): baseii.business_transaction_cycle (Integer) = M2.business_transaction_cycle_id
      JOIN key (VSS)   : vss.business_draft_cycle_130 (String)       = M2.business_transaction_cycle_code
      Campo out: M2.short_description → columna 'trx_cycle' en el output
                 (ej. "Original", "Reversal", "Chargeback")

    Se carga completa y se hace broadcast porque es una tabla pequeña de catálogo.
    """
    path = f"s3://{reference_bucket}/visa_business_transaction_cycle/data.parquet"
    log_info(f"Loading visa_business_transaction_cycle (M2) from: {path}")
    df = spark.read.parquet(path).select(
        F.col("business_transaction_cycle_id").cast(IntegerType()).alias("btc_id"),
        F.col("business_transaction_cycle_code").alias("btc_code"),
        F.col("short_description").alias("trx_cycle")
    )
    count = df.count()
    log_info(f"  Loaded {count:,} transaction cycles")
    return df


def load_exchange_rate(reference_bucket: str, start_date: str, end_date: str) -> DataFrame:
    """
    Carga las tasas de cambio VISA para el rango de fechas indicado.
    En Standard 1.0: operational.dh_exchange_rate_{partition_date} (alias X1 y X2)

    Fuente: exchange-rates-glue/brand=Visa/exchange_date=YYYY-MM-DD/xxx.parquet
    (enriquecido con codigos numericos por glue-exchange-rates, cobertura viva
    y actualizada) — reemplaza a exchange_rate/, fuente manual congelada al
    2026-04-30. Se lee al nivel brand=Visa/ para que Spark descubra
    exchange_date como columna de particion; se renombra a rate_date para no
    tocar el resto del script.

    Esta misma tabla se usa DOS VECES en el join (como X1 y X2 de Standard 1.0):
      · X1: convierte source_currency_code (numérico) → report_currency_code  [para trx_amt]
      · X2: convierte interchange_fee_currency (alfabético) → report_currency_code [para itx_amt]
    Los alias con prefijos x1_ / x2_ se aplican en join_with_exchange_rates() para
    evitar conflictos de columnas en el DataFrame combinado.
    """
    path = f"s3://{reference_bucket}/exchange-rates-glue/brand=Visa/"
    log_info(f"Loading exchange rates from: {path} | range: {start_date} → {end_date}")

    df = spark.read.parquet(path).select(
        F.col("exchange_date").alias("rate_date"),
        F.col("from_currency").alias("currency_from"),
        F.col("to_currency").alias("currency_to"),
        F.col("from_currency_numeric_code").alias("currency_from_code"),
        F.col("fx_rate").alias("exchange_value"),
    ).filter(F.col("rate_date").between(start_date, end_date))

    count = df.count()
    log_info(f"  Loaded {count:,} exchange rate rows for VISA")
    return df


# =============================================================================
# HELPERS: CARGA DE TRANSACCIONES BASEII
# =============================================================================

def load_baseii_transactions(
    operational_bucket: str,
    client_id: str,
    file_type: str,
    start_date: str,
    end_date: str
) -> DataFrame:
    """
    Carga los parquets BaseII del bucket Operational para el cliente, tipo de
    archivo y rango de fechas indicados.

    Estructura S3 (Hive particionado):
      {client_id}/VISA/baseii_drafts/file_type={IN|OUT}/date={YYYY-MM-DD}/

    Spark detecta automáticamente 'file_type' y 'date' como columnas de partición.
    Se filtra por file_type (IN/OUT según issuer_acquirer_indicator) y rango de fechas.

    En Standard 2.0, un único parquet BaseII contiene datos equivalentes a las
    TRES tablas de Standard 1.0:
      · T1 dh_visa_transaction                  → campos de transacción base
      · T2 dh_visa_transaction_calculated_field → campos calculados (CAL stage)
                                                   Incluye: business_transaction_type,
                                                   business_transaction_cycle, jurisdiction,
                                                   reversal_indicator, settlement_report_currency_code
      · T3 dh_visa_interchange                  → resultados de tarifa (ITX stage)
                                                   Identificados por sufijo _itx:
                                                   interchange_fee_amount_itx,
                                                   interchange_fee_currency,
                                                   interchange_fee_descriptor

    La columna de partición 'date' se renombra a 'app_processing_date' para
    alinearse con el naming de Standard 1.0 y con las columnas de output final.
    """
    path = f"s3://{operational_bucket}/{client_id}/VISA/baseii_drafts/"
    log_info(f"Loading BaseII transactions from: {path}")
    log_info(f"  Filter: file_type={file_type} | date: {start_date} → {end_date}")

    df = spark.read.parquet(path) \
        .filter(F.col("file_type") == file_type) \
        .filter(F.col("date").between(start_date, end_date))

    # Renombrar 'date' (partición Hive) → 'app_processing_date'
    # para consistencia con naming de Standard 1.0 y columnas de output
    df = df.withColumnRenamed("date", "app_processing_date")

    # Castear a string explícitamente para asegurar comparaciones correctas en joins
    df = df.withColumn("app_processing_date", F.col("app_processing_date").cast(StringType()))

    count = df.count()
    log_info(f"  Loaded {count:,} BaseII records")
    return df


# =============================================================================
# HELPERS: FILTROS Y TRANSFORMACIONES DE NEGOCIO
# =============================================================================

def apply_hash_filter(df: DataFrame, valid_hashes: list) -> DataFrame:
    """
    Aplica el filtro de content_hash sobre el DataFrame de transacciones (BaseII o VSS).

    Contexto de negocio:
    ─────────────────────
    En Standard 1.0, el parámetro hash_file_filter se inyectaba en el WHERE de
    la CTE filtered_transactions:
        AND app_hash_file IN ('hash1', 'hash2', ...)

    En Standard 2.0, el campo equivalente a app_hash_file es content_hash en el
    parquet de transacciones (BaseII o VSS). La lista de hashes válidos fue obtenida
    de DynamoDB file_control, filtrando por landing_file_name según brand_local_indicator.

    Si la lista de hashes válidos está vacía, se lanza un error para evitar procesar
    una selección incorrecta (podría implicar que no hay archivos para la fecha/filtro
    indicados, lo cual debe corregirse antes de continuar).

    Función genérica: válida para cualquier cliente con hash_file_filter en
    validation_conditions (actualmente solo SBSA para base_ii y vss).
    """
    if not valid_hashes:
        raise ValueError(
            "Hash filter returned 0 valid hashes. "
            "Verify file_control in DynamoDB for the given date range and brand_local_indicator."
        )

    log_info(f"  [HASH_FILTER] Applying content_hash filter: {len(valid_hashes):,} valid hashes")
    filtered = df.filter(F.col("content_hash").isin(valid_hashes))
    count    = filtered.count()
    log_info(f"  [HASH_FILTER] Records after filter: {count:,}")
    return filtered


def join_with_reference_tables(
    df: DataFrame,
    trx_type_df: DataFrame,
    trx_cycle_df: DataFrame
) -> DataFrame:
    """
    Une el DataFrame BaseII con las tablas maestras de tipo y ciclo de transacción.

    En Standard 1.0:
      M1: LEFT JOIN m_visa_business_transaction_type
              ON T2.business_transaction_type = M1.business_transaction_type_id::INTEGER
      M2: LEFT JOIN m_visa_business_transaction_cycle
              ON T2.business_transaction_cycle = M2.business_transaction_cycle_id::INTEGER

    En Standard 2.0:
      · business_transaction_type  en el parquet BaseII es Integer (viene del CAL stage)
      · business_transaction_cycle en el parquet BaseII es Integer (viene del CAL stage)
      · Ambas tablas de referencia son pequeñas → se hace broadcast para evitar shuffles

    Campos resultantes tras el join:
      · trx_type  = M1.short_description  (ej. "Purchase", "Refund", "Cash Advance")
      · trx_cycle = M2.short_description  (ej. "Original", "Reversal", "Chargeback")
    """
    log_info("Joining with visa_business_transaction_type (M1) ...")
    df = df.join(
        F.broadcast(trx_type_df),
        df["business_transaction_type"].cast(IntegerType()) == trx_type_df["btt_id"],
        how="left"
    )

    log_info("Joining with visa_business_transaction_cycle (M2) ...")
    df = df.join(
        F.broadcast(trx_cycle_df),
        df["business_transaction_cycle"].cast(IntegerType()) == trx_cycle_df["btc_id"],
        how="left"
    )

    log_info(f"  Reference joins complete")
    return df


def join_with_exchange_rates(
    df: DataFrame,
    exchange_rate_df: DataFrame,
    report_currency_code: str
) -> DataFrame:
    """
    Une el DataFrame BaseII con las tasas de cambio para calcular los importes
    convertidos a la moneda de reporte del cliente.

    En Standard 1.0 se realizaban DOS joins independientes a la misma tabla de
    tasas de cambio (bajo los alias X1 y X2):

      X1 — Para calcular trx_amt (importe de transacción en moneda de reporte):
        ON X1.app_processing_date = T1.app_processing_date
        AND X1.currency_from_code::INTEGER = T1.source_currency_code::INTEGER  ← cód. NUMÉRICO
        AND X1.currency_to = C1.report_currency_code

      X2 — Para calcular itx_amt (importe de tarifa de intercambio en moneda de reporte):
        ON X2.app_processing_date = T3.app_processing_date
        AND X2.currency_from = T3.fee_currency                                  ← cód. ALFABÉTICO
        AND X2.currency_to = C1.report_currency_code

    Diferencia clave X1 vs X2:
      · X1 usa currency_from_code (Integer ISO numérico) porque la transacción
        almacena el código numérico de la moneda origen (source_currency_code).
      · X2 usa currency_from (String alfabético) porque la tarifa de intercambio
        está expresada en código alfabético (interchange_fee_currency).

    En Standard 2.0:
      · El join de fecha se hace sobre app_processing_date = rate_date (ambos String)
      · Se pre-filtran ambos aliases por report_currency_code (currency_to) para
        reducir el volumen antes del join
      · Todas las columnas se renombran con prefijos x1_ / x2_ para evitar
        conflictos en el DataFrame combinado
      · Se usa broadcast en ambos aliases por ser tablas de referencia pequeñas
    """
    log_info(f"Joining with exchange_rate X1 (numeric code) and X2 (alphabetic code) "
             f"for report_currency: {report_currency_code}")

    # ── Alias X1: join por código NUMÉRICO de moneda origen ───────────────────
    exchange_rate_x1 = exchange_rate_df.select(
        F.col("rate_date").cast(StringType()).alias("x1_rate_date"),
        F.col("currency_from_code").cast(IntegerType()).alias("x1_currency_from_code"),
        F.col("currency_to").alias("x1_currency_to"),
        F.col("exchange_value").cast(DoubleType()).alias("x1_exchange_value")
    ).filter(F.col("x1_currency_to") == report_currency_code)

    # ── Alias X2: join por código ALFABÉTICO de moneda de tarifa ──────────────
    exchange_rate_x2 = exchange_rate_df.select(
        F.col("rate_date").cast(StringType()).alias("x2_rate_date"),
        F.col("currency_from").alias("x2_currency_from"),
        F.col("currency_to").alias("x2_currency_to"),
        F.col("exchange_value").cast(DoubleType()).alias("x2_exchange_value")
    ).filter(F.col("x2_currency_to") == report_currency_code)

    # ── Join X1: source_currency_code (numérico) ↔ currency_from_code ─────────
    df = df.join(
        F.broadcast(exchange_rate_x1),
        (F.col("app_processing_date") == F.col("x1_rate_date")) &
        (F.col("source_currency_code").cast(IntegerType()) == F.col("x1_currency_from_code")),
        how="left"
    ).drop("x1_rate_date", "x1_currency_from_code", "x1_currency_to")

    # ── Join X2: interchange_fee_currency (alfabético) ↔ currency_from ────────
    df = df.join(
        F.broadcast(exchange_rate_x2),
        (F.col("app_processing_date") == F.col("x2_rate_date")) &
        (F.col("interchange_fee_currency") == F.col("x2_currency_from")),
        how="left"
    ).drop("x2_rate_date", "x2_currency_from", "x2_currency_to")

    log_info("  Exchange rate joins complete (x1_exchange_value, x2_exchange_value added)")
    return df


def compute_derived_columns(
    df: DataFrame,
    client_id: str,
    file_type: str,
    brand_local_indicator: str,
    local_currency_code: str,
    report_currency_code: str
) -> DataFrame:
    """
    Calcula las columnas derivadas del output aplicando la lógica de negocio
    de Standard 1.0, ahora expresada como expresiones de columna PySpark.

    Equivalencias por columna:
    ──────────────────────────
    · data_source:
        Literal 'Base II' — igual que el string hardcoded de Standard 1.0.

    · file_source:
        Valor del parámetro brand_local_indicator en minúsculas.
        En Standard 1.0: ''' || LOWER(brand_local_indicator) ||'''::TEXT

    · app_customer_code:
        Parámetro client_id del Glue Job.
        En Standard 1.0: T1.app_customer_code (campo de la tabla transaccional).

    · business_mode:
        'Acquirer' si file_type=OUT (el cliente envía archivos hacia Visa)
        'Issuer'   si file_type=IN  (el cliente recibe archivos desde Visa)
        En Standard 1.0: CASE T1.app_type_file WHEN 'OUT' THEN 'Acquirer' ELSE 'Issuer' END

    · settlement_currency  [lógica iqa_setcur DEFAULT]:
        'Local'   si settlement_report_currency_code = local_currency_code del cliente
        'Foreign' en caso contrario
        En Standard 1.0: CASE WHEN settlement_report_currency_code = C1.local_currency_code ...
        NOTA: la condición iqa_setcur de validation_conditions NO aplica para
        VI/BaseII/validation (no existe ninguna fila configurada). Se usa siempre este default.

    · reversal_indicator_str:
        'N' si reversal_indicator (Integer del parquet) = 0  → transacción original
        'Y' en cualquier otro caso                           → transacción reversada
        En Standard 1.0: CASE WHEN T2.reversal_indicator = 0 THEN 'N' ELSE 'Y' END
        Se nombra 'reversal_indicator_str' para coexistir con la columna Integer
        original; se renombra a 'reversal_indicator' en el output final.

    · fee_descriptor:
        TRIM(interchange_fee_descriptor) del parquet BaseII.
        En Standard 1.0 venía del JOIN a m_interchange_rules_visa (M3). En Standard 2.0
        este campo ya está pre-calculado y embebido en el parquet como interchange_fee_descriptor.

    · report_currency_code:
        Literal con la moneda de reporte del cliente (de DynamoDB m_client).
        En Standard 1.0: C1.report_currency_code (del JOIN a t_customer).
    """
    log_info("Computing derived business columns...")

    # business_mode: fijo para toda la ejecución según parámetro issuer_acquirer_indicator
    business_mode_val = "Acquirer" if file_type == "OUT" else "Issuer"

    df = df \
        .withColumn("data_source",       F.lit("Base II")) \
        .withColumn("file_source",       F.lit(brand_local_indicator.lower())) \
        .withColumn("app_customer_code", F.lit(client_id)) \
        .withColumn("business_mode",     F.lit(business_mode_val)) \
        .withColumn(
            # settlement_currency: 'Local' vs 'Foreign'
            # Lógica iqa_setcur DEFAULT (la única aplicable para VI/BaseII/validation)
            "settlement_currency",
            F.when(
                F.col("settlement_report_currency_code") == F.lit(local_currency_code),
                F.lit("Local")
            ).otherwise(F.lit("Foreign"))
        ) \
        .withColumn(
            # reversal_indicator como String ('N'/'Y') para el output
            # Se nombra _str para coexistir con el Integer original del parquet
            "reversal_indicator_str",
            F.when(F.col("reversal_indicator") == 0, F.lit("N")).otherwise(F.lit("Y"))
        ) \
        .withColumn(
            # fee_descriptor: limpiamos espacios del descriptor pre-calculado en el parquet
            # (equivalente al TRIM(M3.fee_descriptor) de Standard 1.0)
            "fee_descriptor",
            F.trim(F.col("interchange_fee_descriptor"))
        ) \
        .withColumn("report_currency_code", F.lit(report_currency_code))

    log_info("  Derived columns computed")
    return df


def aggregate_results(df: DataFrame) -> DataFrame:
    """
    Agrupa y agrega los datos para producir el resumen de Data Quality transaccional.
    Equivale al SELECT con GROUP BY 1,2,...,12 de la función get_visa_validation_results_baseii
    de Standard 1.0.

    Columnas de agrupación (12 dimensiones):
      app_processing_date, data_source, file_source, app_customer_code,
      business_mode, jurisdiction, settlement_currency, reversal_indicator,
      trx_type, trx_cycle, fee_descriptor, report_currency_code

    Métricas calculadas (3 medidas):
    ──────────────────────────────────
    · trx_count: COUNT(*) — número de transacciones en el grupo
        En Standard 1.0: COUNT(1)::NUMERIC

    · trx_amt: SUM(COALESCE(X1.exchange_value, 1) * source_amount)
        Importe total de las transacciones en moneda de reporte.
        Si no hay tasa X1 disponible → COALESCE devuelve 1.0 → importe sin conversión.
        source_amount es el importe original de la transacción (campo T1 en Standard 1.0).

    · itx_amt: SUM(COALESCE(X2.exchange_value, X1.exchange_value, 1) * interchange_fee_amount_itx)
        Importe total de tarifas de intercambio en moneda de reporte.
        Cascada de tasas:
          1. X2: tasa de la moneda de la tarifa (interchange_fee_currency → report_currency)
          2. X1: tasa de la moneda de la transacción (source_currency → report_currency) como fallback
          3. 1.0: sin conversión si ninguna tasa está disponible
        interchange_fee_amount_itx es el importe de tarifa calculado (campo T3.calculated_value
        en Standard 1.0).
    """
    log_info("Aggregating results (GROUP BY 12 dimensions)...")

    # Las 12 columnas de agrupación del GROUP BY de Standard 1.0
    group_by_cols = [
        "app_processing_date",   # 1 - fecha de procesamiento (partición Hive 'date=')
        "data_source",           # 2 - siempre 'Base II'
        "file_source",           # 3 - brand_local_indicator
        "app_customer_code",     # 4 - client_id
        "business_mode",         # 5 - 'Acquirer' o 'Issuer'
        "jurisdiction",          # 6 - campo calculado del parquet (CAL stage)
        "settlement_currency",   # 7 - 'Local' o 'Foreign'
        "reversal_indicator_str",# 8 - 'N' o 'Y' (renombrado a 'reversal_indicator' al final)
        "trx_type",              # 9 - short_description de M1 (puede ser null si no match)
        "trx_cycle",             # 10 - short_description de M2 (puede ser null si no match)
        "fee_descriptor",        # 11 - TRIM(interchange_fee_descriptor)
        "report_currency_code",  # 12 - moneda de reporte del cliente
    ]

    result = df.groupBy(group_by_cols).agg(
        # trx_count: número de transacciones en el grupo
        F.count(F.lit(1)).cast(DoubleType()).alias("trx_count"),

        # trx_amt: importe de transacciones convertido a moneda de reporte (via X1)
        # COALESCE(x1_exchange_value, 1.0) protege contra tasas de cambio no disponibles
        F.sum(
            F.coalesce(F.col("x1_exchange_value"), F.lit(1.0)) *
            F.coalesce(F.col("source_amount"), F.lit(0.0))
        ).alias("trx_amt"),

        # itx_amt: importe de tarifa de intercambio convertido a moneda de reporte
        # Cascada: X2 (moneda de tarifa) → X1 (moneda de transacción) → 1.0 (sin conversión)
        F.sum(
            F.coalesce(
                F.col("x2_exchange_value"),
                F.col("x1_exchange_value"),
                F.lit(1.0)
            ) *
            F.coalesce(F.col("interchange_fee_amount_itx"), F.lit(0.0))
        ).alias("itx_amt"),
    )

    # Renombrar 'reversal_indicator_str' → 'reversal_indicator' en el output final
    # (alineado con el naming del Standard 1.0 y con data_visa_validation_result.csv)
    result = result.withColumnRenamed("reversal_indicator_str", "reversal_indicator")

    count = result.count()
    log_info(f"  Aggregation complete: {count:,} result rows")
    return result


# =============================================================================
# HELPERS: CARGA Y PROCESAMIENTO VSS (FASE 2)
# =============================================================================

def load_vss_transactions(
    operational_bucket: str,
    client_id: str,
    start_date: str,
    end_date: str
) -> DataFrame:
    """
    Carga los parquets VSS 130 del bucket Operational para el cliente y rango de fechas.

    A diferencia de BaseII (IN para Issuer, OUT para Acquirer), el VSS solo tiene
    file_type=IN — un único archivo de liquidación que contiene registros de ambos modos.
    La distinción Acquirer/Issuer dentro del VSS se hace por el campo business_mode_130:
      '1' = Acquirer,  '2' = Issuer

    En Standard 1.0: dh_visa_transaction_vss_130_{cliente}_in_{YYYYMMDD}
    En Standard 2.0: {client_id}/VISA/vss_130/file_type=IN/date={YYYY-MM-DD}/

    La columna de partición 'date' se renombra a 'app_processing_date' para alinearse
    con el naming de Standard 1.0 y con las columnas de output final.
    """
    path = f"s3://{operational_bucket}/{client_id}/VISA/vss_130/"
    log_info(f"Cargando transacciones VSS 130 desde: {path}")
    log_info(f"  Filtro: file_type=IN | fechas: {start_date} → {end_date}")

    df = spark.read.parquet(path) \
        .filter(F.col("file_type") == "IN") \
        .filter(F.col("date").between(start_date, end_date))

    df = df.withColumnRenamed("date", "app_processing_date")
    df = df.withColumn("app_processing_date", F.col("app_processing_date").cast(StringType()))

    count = df.count()
    log_info(f"  Cargados {count:,} registros VSS 130 (todos los modos)")
    return df


def apply_vss_base_filters(df: DataFrame, business_mode_code: str) -> DataFrame:
    """
    Aplica los filtros fijos del WHERE de la CTE filtered_transactions de Standard 1.0.

    Filtros replicados de get_visa_validation_results_vss():
      · summary_level_130 = '10'           → nivel de detalle; descarta rollups (00, 01...)
      · vss_aggregation_level = 0          → equivalente a T2.aggregation_level='0';
                                             en Standard 2.0 ya viene en el parquet principal
      · no_data_indicator_130 != 'Y'       → registros con datos reales
      · business_draft_type_130 IN (...)   → tipos de transacción válidos para VSS
      · business_mode_130 = business_mode  → Acquirer ('1') o Issuer ('2')

    Nota: el filtro M2.short_description='Original' (trx_cycle) se aplica DESPUÉS
    del join con la maestra de ciclos en join_vss_with_reference_tables().
    """
    tipos_validos = ["100", "110", "120", "200", "210", "300", "310", "330"]

    df = df \
        .filter(F.col("summary_level_130")      == "10") \
        .filter(F.col("vss_aggregation_level").cast(LongType()) == 0) \
        .filter(F.col("no_data_indicator_130")  != "Y") \
        .filter(F.col("business_draft_type_130").isin(tipos_validos)) \
        .filter(F.col("business_mode_130")      == business_mode_code)

    count = df.count()
    log_info(f"  Registros VSS tras filtros base (mode={business_mode_code}): {count:,}")
    return df


def join_vss_with_reference_tables(
    df: DataFrame,
    trx_type_df: DataFrame,
    trx_cycle_df: DataFrame
) -> DataFrame:
    """
    Une el DataFrame VSS con las maestras de tipo y ciclo de transacción.

    Diferencia clave respecto a BaseII:
      BaseII → join por ID (Integer):
        business_transaction_type (Int) = btt_id
      VSS    → join por CÓDIGO (String):
        business_draft_type_130         = btt_code  (ej. '100', '200')
        business_draft_cycle_130        = btc_code  (ej. '1', '2')

    En Standard 1.0:
      M1: ON T1.business_transaction_type_130 = M1.business_transaction_type_code
      M2: ON T1.business_transaction_cycle_130 = M2.business_transaction_cycle_code

    En Standard 2.0, los campos renombrados son:
      business_draft_type_130  → equivalente a business_transaction_type_130
      business_draft_cycle_130 → equivalente a business_transaction_cycle_130

    Post-join: filtro obligatorio trx_cycle = 'Original' (requerido por Standard 1.0
    para excluir reversales, contracargos y otros ciclos en la vista de liquidación).
    """
    log_info("VSS: Uniendo con visa_business_transaction_type por código (M1) ...")
    df = df.join(
        F.broadcast(trx_type_df),
        df["business_draft_type_130"] == trx_type_df["btt_code"],
        how="left"
    )

    log_info("VSS: Uniendo con visa_business_transaction_cycle por código (M2) ...")
    df = df.join(
        F.broadcast(trx_cycle_df),
        df["business_draft_cycle_130"] == trx_cycle_df["btc_code"],
        how="left"
    )

    # Filtro post-join: equivalente a "AND M2.short_description = 'Original'" de Standard 1.0
    df = df.filter(F.col("trx_cycle") == "Original")
    count = df.count()
    log_info(f"  VSS: Registros tras filtro trx_cycle='Original': {count:,}")
    return df


def join_vss_with_exchange_rates(
    df: DataFrame,
    exchange_rate_df: DataFrame,
    report_currency_code: str
) -> DataFrame:
    """
    Aplica los dos joins de tipo de cambio del VSS en dos pasos, replicando la lógica
    de Standard 1.0 que usa dh_exchange_rate en aux_vss (Paso 1) y output_vss (Paso 2).

    Paso 1 — aux_vss (obtener código ALFABÉTICO de la moneda de liquidación):
      Standard 1.0: JOIN dh_exchange_rate X1
        ON settlement_currency_code_130 = X1.currency_from_code
        AND C1.report_currency_code = X1.currency_to
      Resultado: settlement_currency = COALESCE(X1.currency_from, report_currency_code)
      Columna generada: 'vss_settlement_currency_alpha' (alfabético, p. ej. 'USD', 'EUR')

    Paso 2 — output_vss (obtener TASA para convertir importes a moneda de reporte):
      Standard 1.0: JOIN dh_exchange_rate X1
        ON currency_from = T1.settlement_currency (alfabético)
        AND currency_to = C1.report_currency_code
      Resultado: exchange_value → se aplica como multiplicador sobre trx_amt e itx_amt
      Columna generada: 'vss_x1_rate'

    Ambos pasos usan la misma tabla exchange_rate_df (ya cargada y filtrada por VISA).
    """
    # ── Paso 1: código NUMÉRICO → código ALFABÉTICO de moneda de liquidación ───
    # Equivale al JOIN X1 de aux_vss en Standard 1.0
    xr_step1 = exchange_rate_df.select(
        F.col("currency_from_code").cast(LongType()).alias("xr1_from_code"),
        F.col("currency_to").alias("xr1_to"),
        F.col("currency_from").alias("xr1_currency_from"),
    ).filter(F.col("xr1_to") == report_currency_code)

    df = df.join(
        F.broadcast(xr_step1),
        df["settlement_currency_code_130"].cast(LongType()) == xr_step1["xr1_from_code"],
        how="left"
    ).drop("xr1_from_code", "xr1_to")

    # Moneda de liquidación en formato alfabético; fallback al report_currency si no hay tasa
    # Equivale a COALESCE(X1.currency_from, C1.report_currency_code) de Standard 1.0
    df = df.withColumn(
        "vss_settlement_currency_alpha",
        F.coalesce(F.col("xr1_currency_from"), F.lit(report_currency_code))
    ).drop("xr1_currency_from")

    # ── Paso 2: código ALFABÉTICO → tasa de conversión a moneda de reporte ─────
    # Equivale al JOIN X1 de output_vss en Standard 1.0
    xr_step2 = exchange_rate_df.select(
        F.col("currency_from").alias("xr2_from"),
        F.col("currency_to").alias("xr2_to"),
        F.col("exchange_value").cast(DoubleType()).alias("vss_x1_rate"),
    ).filter(F.col("xr2_to") == report_currency_code)

    df = df.join(
        F.broadcast(xr_step2),
        df["vss_settlement_currency_alpha"] == xr_step2["xr2_from"],
        how="left"
    ).drop("xr2_from", "xr2_to")

    log_info(f"  VSS: Joins de tipo de cambio completados (report_currency={report_currency_code})")
    return df


def compute_vss_derived_columns(
    df: DataFrame,
    client_id: str,
    business_mode_code: str,
    brand_local_indicator: str,
    local_currency_code: str,
    report_currency_code: str
) -> DataFrame:
    """
    Calcula las columnas derivadas de negocio del output VSS.
    Equivale a la SELECT de aux_vss + output_vss en Standard 1.0.

    Diferencias clave respecto a compute_derived_columns() (BaseII):

    · business_mode: derivado del campo business_mode_130 del parquet (no del file_type
      del path), porque VSS siempre es file_type=IN y la distinción modo viene del campo.
      '1' → 'Acquirer',  '2' → 'Issuer'

    · jurisdiction: calculada aquí (en BaseII viene pre-calculada en el parquet CAL).
      Lógica del CASE en Standard 1.0:
        jurisdiction_code_130 = '00'            → 'interregional'
        jurisdiction_code_130 != '00' y src≠dst → 'intraregional'
        jurisdiction_code_130 != '00' y src=dst → 'off-us'

    · settlement_currency: compara vss_settlement_currency_alpha (obtenida en el
      Paso 1 del join de tipo de cambio) con local_currency_code del cliente.
      Si coinciden → 'Local', si no → 'Foreign'.
      (En BaseII se comparaba settlement_report_currency_code directamente.)

    · reversal_indicator: viene como string 'N'/'Y' directamente en el parquet VSS.
      (En BaseII era un Integer 0/1 que se convertía aquí.)

    · fee_descriptor: TRIM(fee_level_descriptor)
      (En BaseII era TRIM(interchange_fee_descriptor) — campo diferente.)
    """
    log_info("VSS: Calculando columnas derivadas de negocio ...")

    df = df \
        .withColumn("data_source",      F.lit("VSS")) \
        .withColumn("file_source",       F.lit(brand_local_indicator.lower())) \
        .withColumn("app_customer_code", F.lit(client_id)) \
        .withColumn(
            "business_mode",
            F.when(F.col("business_mode_130") == "1", F.lit("Acquirer"))
             .when(F.col("business_mode_130") == "2", F.lit("Issuer"))
             .otherwise(F.lit(""))
        ) \
        .withColumn(
            "jurisdiction",
            F.when(F.col("jurisdiction_code_130") == "00", F.lit("interregional"))
             .when(
                 (F.col("jurisdiction_code_130") != "00") &
                 (F.col("source_country_code_130") != F.col("destination_country_code_130")),
                 F.lit("intraregional")
             )
             .when(
                 (F.col("jurisdiction_code_130") != "00") &
                 (F.col("source_country_code_130") == F.col("destination_country_code_130")),
                 F.lit("off-us")
             )
             .otherwise(F.lit(""))
        ) \
        .withColumn(
            "settlement_currency",
            F.when(F.col("vss_settlement_currency_alpha") == F.lit(local_currency_code), F.lit("Local"))
             .otherwise(F.lit("Foreign"))
        ) \
        .withColumn("reversal_indicator",   F.col("reversal_indicator_130")) \
        .withColumn("fee_descriptor",       F.trim(F.col("fee_level_descriptor"))) \
        .withColumn("report_currency_code", F.lit(report_currency_code))

    log_info("  VSS: Columnas derivadas calculadas")
    return df


def aggregate_vss_results(df: DataFrame) -> DataFrame:
    """
    Agrega los resultados VSS con el mismo GROUP BY de 12 dimensiones que BaseII.
    Equivale al SELECT de output_vss con GROUP BY 1..12 en Standard 1.0.

    Diferencia crítica respecto a aggregate_results() (BaseII):
      BaseII: trx_count = COUNT(*)    — cuenta filas individuales de transacción
      VSS:    trx_count = SUM(count_130) — el VSS ya viene pre-agregado por Visa;
              count_130 contiene el número de transacciones que representa cada fila

    Montos:
      trx_amt = SUM(interchange_amount_settlement_currency_130 * COALESCE(vss_x1_rate, 1.0))
      itx_amt = SUM((reimbursement_fee_credits_settlement_currency
                   + reimbursement_fee_debits_settlement_currency) * COALESCE(vss_x1_rate, 1.0))

    La tasa vss_x1_rate se obtiene del Paso 2 del join de tipo de cambio VSS
    (currency_from=settlement_currency_alpha → currency_to=report_currency_code).
    Si no hay tasa disponible, COALESCE usa 1.0 → importes sin conversión.
    """
    log_info("VSS: Agregando resultados (GROUP BY 12 dimensiones) ...")

    group_by_cols = [
        "app_processing_date",
        "data_source",
        "file_source",
        "app_customer_code",
        "business_mode",
        "jurisdiction",
        "settlement_currency",
        "reversal_indicator",
        "trx_type",
        "trx_cycle",
        "fee_descriptor",
        "report_currency_code",
    ]

    rate = F.coalesce(F.col("vss_x1_rate"), F.lit(1.0))

    result = df.groupBy(group_by_cols).agg(
        # trx_count: suma de count_130 (pre-agregado por Visa en el VSS)
        F.round(F.sum(F.col("count_130").cast(DoubleType())), 2)
         .cast(DecimalType(18, 2)).alias("trx_count"),

        # trx_amt: importe de transacción (settlement currency → report currency)
        F.round(
            F.sum(F.col("interchange_amount_settlement_currency_130").cast(DoubleType()) * rate),
            2
        ).cast(DecimalType(18, 2)).alias("trx_amt"),

        # itx_amt: créditos + débitos de reembolso (settlement currency → report currency)
        F.round(
            F.sum(
                (F.col("reimbursement_fee_credits_settlement_currency").cast(DoubleType()) +
                 F.col("reimbursement_fee_debits_settlement_currency").cast(DoubleType())) * rate
            ),
            2
        ).cast(DecimalType(18, 2)).alias("itx_amt"),
    )

    count = result.count()
    log_info(f"  VSS: Agregación completa → {count:,} filas de resultado")
    return result


# =============================================================================
# MAIN
# =============================================================================

def main():
    """
    Punto de entrada del Glue job glue-vi-data-quality. Resuelve los
    argumentos del job (incluyendo las listas multi-valor client_id e
    issuer_acquirer_indicator, y el parámetro opcional
    brand_local_override leído directamente de sys.argv), carga las
    tablas de referencia comunes (validation_conditions,
    visa_business_transaction_type, visa_business_transaction_cycle,
    exchange rates) y, para cada combinación de cliente ×
    issuer_acquirer_indicator × brand_local, ejecuta el pipeline BaseII
    (equivalente a get_visa_validation_results_baseii()) y el pipeline
    VSS (equivalente a get_visa_validation_results_vss()), acumulando los
    resultados parciales. Al final de cada cliente, une todos los
    parciales y escribe un único Parquet de resumen a
    s3://{analytics_bucket}/{client_id}/reports/tst_{client_id}_data_quality.parquet.

    Returns:
        None. Llama a job.commit() al finalizar.

    Ejemplo:
        main()  # invocado automáticamente al ejecutar el script como Glue job
    """
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "client_id",
        "issuer_acquirer_indicator",
        "start_date",
        "end_date",
        "operational_bucket",
        "reference_bucket",
        "analytics_bucket",
        "dynamodb_table_client",
        "dynamodb_table_file_control",
    ])

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # ── Parámetros del Glue Job ───────────────────────────────────────────────
    client_ids_raw              = args["client_id"]                   # "EBGR" o "EBGR,SBSA"
    issuer_acquirer_raw         = args["issuer_acquirer_indicator"]    # "A" o "A,I"
    start_date                  = args["start_date"]                  # YYYY-MM-DD
    end_date                    = args["end_date"]                    # YYYY-MM-DD
    operational_bucket          = args["operational_bucket"]
    reference_bucket            = args["reference_bucket"]
    analytics_bucket            = args["analytics_bucket"]
    dynamodb_table_client       = args["dynamodb_table_client"]
    dynamodb_table_file_control = args["dynamodb_table_file_control"]

    # ── Parsear listas de parámetros multi-valor ──────────────────────────────
    client_ids           = [c.strip().upper() for c in client_ids_raw.split(",") if c.strip()]
    issuer_acquirer_list = [ia.strip().upper() for ia in issuer_acquirer_raw.split(",") if ia.strip()]

    # ── Parámetro opcional: brand_local_override ───────────────────────────────
    # No se usa getResolvedOptions() porque ese método lanza error si el argumento
    # no está presente. En su lugar se revisa sys.argv directamente para detectar
    # si el parámetro fue pasado o no en este run.
    brand_local_override = None
    if "--brand_local_override" in sys.argv:
        idx = sys.argv.index("--brand_local_override")
        brand_local_override = sys.argv[idx + 1].strip().lower()

    log_info("=" * 70)
    log_info("VI-DATA-QUALITY (BaseII + VSS) - STARTING")
    log_info("=" * 70)
    log_info(f"Clients                : {client_ids}")
    log_info(f"Issuer/Acquirer list   : {issuer_acquirer_list}")
    log_info(f"Brand/Local Override   : {brand_local_override if brand_local_override else '(no aplica — usando validation_conditions)'}")
    log_info(f"Date Range             : {start_date} → {end_date}")
    log_info(f"Operational Bucket     : {operational_bucket}")
    log_info(f"Reference Bucket       : {reference_bucket}")
    log_info(f"Analytics Bucket       : {analytics_bucket}")
    log_info(f"DynamoDB Client Table  : {dynamodb_table_client}")
    log_info(f"DynamoDB FileCtrl Table: {dynamodb_table_file_control}")
    log_info("=" * 70)

    # ── Tablas de referencia S3 — cargadas UNA SOLA VEZ fuera de todos los loops ──
    # Equivale a los joins M1/M2 y X1/X2 de Standard 1.0: tablas pequeñas reutilizadas
    # por todos los clientes/modos. Cargarlas dentro del loop desperdiciaría recursos.
    log_info("")
    log_info("[INIT] Loading shared reference tables (S3) — loaded once for all clients/modes")
    conditions_df    = load_validation_conditions(reference_bucket).cache()
    trx_type_df      = load_business_transaction_type(reference_bucket).cache()
    trx_cycle_df     = load_business_transaction_cycle(reference_bucket).cache()
    exchange_rate_df = load_exchange_rate(reference_bucket, start_date, end_date).cache()

    # ── Loop principal: equivale al FOREACH customer_code de Standard 1.0 ─────
    # Un Parquet de salida por client_id (union de todos los modos/brand_locals).
    for client_id in client_ids:

        log_info("")
        log_info("=" * 70)
        log_info(f"[CLIENT] Processing: {client_id}")
        log_info("=" * 70)

        # Datos del cliente desde DynamoDB — una vez por cliente
        # Equivale al JOIN LEFT control.t_customer C1 de Standard 1.0
        log_info(f"  Loading client master data (DynamoDB) for {client_id}")
        client_data          = get_client_data(client_id, dynamodb_table_client)
        local_currency_code  = client_data["local_currency_code"]
        report_currency_code = client_data["report_currency_code"]

        accumulated_dfs = []

        # ── Loop de modo: equivale al FOREACH issuer_acquirer_ind de Standard 1.0 ──
        for ia_indicator in issuer_acquirer_list:

            # Homologación: A → Acquirer/OUT/mode=1  |  I → Issuer/IN/mode=2
            file_type          = "OUT" if ia_indicator == "A" else "IN"
            business_mode_code = "1"   if ia_indicator == "A" else "2"

            log_info("")
            log_info(f"  [MODE] ia_indicator={ia_indicator} → "
                     f"file_type={file_type}, business_mode={business_mode_code}")

            # ── Bloque BaseII ──────────────────────────────────────────────────
            # Equivale a get_visa_validation_results_baseii() de Standard 1.0.
            # Determinar los valores de brand_local a iterar (configurados en
            # validation_conditions o sobreescritos vía --brand_local_override).
            brand_local_values_baseii = get_brand_local_values(
                conditions_df      = conditions_df,
                client_id          = client_id,
                data_source        = "base_ii",
                business_mode_code = business_mode_code,
                override           = brand_local_override,
            )

            for brand_local in brand_local_values_baseii:

                log_info(f"    [BASEII/BRAND_LOCAL] Processing brand_local='{brand_local}'")

                # Hash filter BaseII
                needs_hash_filter = check_hash_filter_needed(
                    conditions_df      = conditions_df,
                    client_id          = client_id,
                    data_source        = "base_ii",
                    business_mode_code = business_mode_code,
                )

                valid_hashes = []
                if needs_hash_filter and brand_local.lower() != "default":
                    log_info(f"      Hash filter requerido — consultando DynamoDB file_control")
                    valid_hashes = get_valid_content_hashes(
                        client_id                   = client_id,
                        brand_local_indicator       = brand_local,
                        start_date                  = start_date,
                        end_date                    = end_date,
                        dynamodb_table_file_control = dynamodb_table_file_control,
                    )
                elif needs_hash_filter and brand_local.lower() == "default":
                    log_info(f"      Hash filter configurado pero brand_local='default'"
                             f" → omitido (todos los archivos incluidos)")
                else:
                    log_info(f"      Sin hash_file_filter configurado → sin filtro de hash")

                # Cargar transacciones BaseII para este cliente/modo
                baseii_df = load_baseii_transactions(
                    operational_bucket = operational_bucket,
                    client_id          = client_id,
                    file_type          = file_type,
                    start_date         = start_date,
                    end_date           = end_date,
                )

                # Aplicar filtro de hash si aplica
                if valid_hashes:
                    baseii_df = apply_hash_filter(baseii_df, valid_hashes)

                # Condición WHERE adicional desde validation_conditions
                # Para VI/BaseII/validation actualmente no hay filas configuradas → no-op
                baseii_df = apply_where_condition(
                    df                 = baseii_df,
                    conditions_df      = conditions_df,
                    client_id          = client_id,
                    data_source        = "base_ii",
                    business_mode_code = business_mode_code,
                )

                # Joins con maestras de tipo/ciclo de transacción (M1/M2)
                baseii_df = join_with_reference_tables(baseii_df, trx_type_df, trx_cycle_df)

                # Joins con tasas de cambio (X1: código numérico, X2: código alfabético)
                baseii_df = join_with_exchange_rates(baseii_df, exchange_rate_df, report_currency_code)

                # Columnas derivadas de negocio
                baseii_df = compute_derived_columns(
                    df                    = baseii_df,
                    client_id             = client_id,
                    file_type             = file_type,
                    brand_local_indicator = brand_local,
                    local_currency_code   = local_currency_code,
                    report_currency_code  = report_currency_code,
                )

                # Agregación (GROUP BY 12 dimensiones)
                partial_df = aggregate_results(baseii_df)
                accumulated_dfs.append(partial_df)
                log_info(f"    [BASEII/BRAND_LOCAL] Listo — parcial acumulado")

            # ── Bloque VSS ─────────────────────────────────────────────────────
            # Equivale a get_visa_validation_results_vss() de Standard 1.0.
            # VSS siempre lee file_type=IN; la distinción modo se hace por business_mode_130.
            log_info("")
            log_info(f"    [VSS] ia_indicator={ia_indicator} → business_mode={business_mode_code}")

            brand_local_values_vss = get_brand_local_values(
                conditions_df      = conditions_df,
                client_id          = client_id,
                data_source        = "vss",
                business_mode_code = business_mode_code,
                override           = brand_local_override,
            )

            for brand_local in brand_local_values_vss:

                log_info(f"    [VSS/BRAND_LOCAL] Processing brand_local='{brand_local}'")

                # Hash filter VSS
                needs_hash_filter_vss = check_hash_filter_needed(
                    conditions_df      = conditions_df,
                    client_id          = client_id,
                    data_source        = "vss",
                    business_mode_code = business_mode_code,
                )

                valid_hashes_vss = []
                if needs_hash_filter_vss and brand_local.lower() != "default":
                    log_info(f"      VSS: Hash filter requerido — consultando DynamoDB file_control")
                    valid_hashes_vss = get_valid_content_hashes(
                        client_id                   = client_id,
                        brand_local_indicator       = brand_local,
                        start_date                  = start_date,
                        end_date                    = end_date,
                        dynamodb_table_file_control = dynamodb_table_file_control,
                    )
                elif needs_hash_filter_vss and brand_local.lower() == "default":
                    log_info(f"      VSS: Hash filter configurado pero brand_local='default' → omitido")
                else:
                    log_info(f"      VSS: Sin hash_file_filter configurado → sin filtro de hash")

                # Cargar transacciones VSS 130 (siempre file_type=IN)
                vss_df = load_vss_transactions(
                    operational_bucket = operational_bucket,
                    client_id          = client_id,
                    start_date         = start_date,
                    end_date           = end_date,
                )

                # Filtros fijos del WHERE de Standard 1.0 (summary_level, aggregation_level, etc.)
                vss_df = apply_vss_base_filters(vss_df, business_mode_code)

                # Aplicar filtro de hash si aplica
                if valid_hashes_vss:
                    vss_df = apply_hash_filter(vss_df, valid_hashes_vss)

                # Condición WHERE adicional desde validation_conditions
                # Ej: NGGR/vss/mode=1 → rollup_to_sre_identifier_130 IN ('1000737016','1000737015')
                vss_df = apply_where_condition(
                    df                 = vss_df,
                    conditions_df      = conditions_df,
                    client_id          = client_id,
                    data_source        = "vss",
                    business_mode_code = business_mode_code,
                )

                # Joins con maestras (por código String, no por ID Integer como en BaseII)
                vss_df = join_vss_with_reference_tables(vss_df, trx_type_df, trx_cycle_df)

                # Joins de tipo de cambio VSS (dos pasos: numérico→alfabético, luego tasa)
                vss_df = join_vss_with_exchange_rates(vss_df, exchange_rate_df, report_currency_code)

                # Columnas derivadas de negocio
                vss_df = compute_vss_derived_columns(
                    df                    = vss_df,
                    client_id             = client_id,
                    business_mode_code    = business_mode_code,
                    brand_local_indicator = brand_local,
                    local_currency_code   = local_currency_code,
                    report_currency_code  = report_currency_code,
                )

                # Agregación (mismo GROUP BY que BaseII; SUM(count_130) en lugar de COUNT(*))
                partial_vss_df = aggregate_vss_results(vss_df)
                accumulated_dfs.append(partial_vss_df)
                log_info(f"    [VSS/BRAND_LOCAL] Listo — parcial acumulado")

        # ── Unión de todos los parciales del cliente ───────────────────────────
        if not accumulated_dfs:
            log_info(f"  [WARNING] No results generated for client {client_id} — skipping write")
            continue

        log_info(f"  Merging {len(accumulated_dfs)} partial result(s) for {client_id}")
        result_df = accumulated_dfs[0]
        for partial in accumulated_dfs[1:]:
            result_df = result_df.unionByName(partial)

        result_df = result_df.cache()

        # Guardar resultado por client_id
        output_path = f"s3://{analytics_bucket}/{client_id}/reports/tst_{client_id}_data_quality.parquet"
        log_info(f"  Saving output → {output_path}")
        save_parquet(result_df, output_path)

        total_records = result_df.count()
        result_df.unpersist()

        log_info(f"  [CLIENT DONE] {client_id} ({client_data['client_name']}) "
                 f"| total rows: {total_records:,} | path: {output_path}")

    # ── Liberar caché de tablas de referencia ─────────────────────────────────
    conditions_df.unpersist()
    trx_type_df.unpersist()
    trx_cycle_df.unpersist()
    exchange_rate_df.unpersist()

    log_info("")
    log_info("=" * 70)
    log_info("VI-DATA-QUALITY (BaseII + VSS) - COMPLETED")
    log_info("=" * 70)

    job.commit()


if __name__ == "__main__":
    main()
