"""
format_exchange_rates.py — Job real: itl-0004-itx-dev-intchg-02-glue-exchange-rates
================================================================================
Archivo:     glue/scripts/reports/exchange_rates/format_exchange_rates.py
S3 Script:   s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/format_exchange_rates.py

Enriquece la tabla de tasas de cambio scrapeadas (exchange_rates, con solo
códigos alfabéticos de moneda) cruzándola contra el maestro de monedas
(m_currency) para resolver los códigos numéricos. Escribe el resultado
particionado por brand y exchange_date — solo toca las particiones
presentes en la corrida actual (nuevas o recalculadas por un archivo fuente
corregido), dejando intactas las que no cambiaron.

Escritura y registro en catálogo: antes de escribir se purga en S3
(glueContext.purge_s3_path, operación S3 pura) el prefijo de cada partición
que esta corrida va a tocar — así, si una fecha ya procesada se sobrescribe
en el origen, el bookmark detecta el archivo modificado, esta corrida vuelve
a leerla y reemplaza esa partición en destino en vez de duplicarla. La
escritura y el registro en el catálogo Glue usan el sink nativo
(enableUpdateCatalog=True) en vez de una llamada boto3 directa al API de
Glue — necesario porque la VPC del job no tiene ruta de salida al endpoint
público de Glue (sí a S3, vía Gateway Endpoint), así que un
boto3.client("glue") desde el driver del job da ConnectTimeoutError.

Flujo:
  1. Lectura bookmarked de exchange_rates (solo archivos nuevos o
     modificados desde la última corrida exitosa) y lectura completa de
     m_currency (maestro pequeño que puede evolucionar, sin bookmark).
  2. Normalización del maestro: un registro por código alfabético,
     deduplicado.
  3. Cruce left join por from_currency y to_currency hacia códigos
     numéricos.
  4. Reparticionamiento por brand/exchange_date.
  5. Si hay datos: purga en S3 cada partición (brand, exchange_date) que
     esta corrida va a tocar, y escribe + registra en el catálogo con el
     sink nativo de Glue. Si el bookmark no trajo nada nuevo, no escribe
     nada (evita jobs vacíos con solo overhead de arranque).

Job Bookmarks: habilitados solo para exchange_rates (--job-bookmark-option
job-bookmark-enable + transformation_ctx). m_currency_csv se relee completo
en cada corrida, ya que es un maestro pequeño que puede cambiar.

Database / Input / Output:
  Database: itl_0004_itx_dev_02_glue_database_exchange_rates
  Input:
    - exchange_rates (bookmarked, particionada por brand, exchange_date;
      contiene from_currency, to_currency, exchange_rate)
    - m_currency_csv (maestro pequeño: currency_alphabetic_code,
      currency_numeric_code)
  Output:
    - exchange-rates-glue (destino S3 + tabla catálogo)
    - Mismo particionado que input + 2 columnas nuevas:
      from_currency_numeric_code, to_currency_numeric_code

Requiere que ambas tablas de entrada ya estén catalogadas en la base de datos
indicada.
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
DATABASE             = "itl_0004_itx_dev_02_glue_database_exchange_rates"
EXCHANGE_RATES_TABLE = "exchange_rates"
CURRENCY_TABLE       = "m_currency_csv"
TARGET_TABLE         = "exchange-rates-glue"
TARGET_S3_PATH       = "s3://itl-0004-itx-dev-intchg-02-s3-reference/exchange-rates-glue/"
PARTITION_COLS       = ["brand", "exchange_date"]

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# 1. Leer tablas fuente desde el Glue Data Catalog
# ---------------------------------------------------------------------------
exchange_rates_df = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=EXCHANGE_RATES_TABLE,
    transformation_ctx="exchange_rates_bookmark",
).toDF()

currency_df = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=CURRENCY_TABLE,
).toDF()

# Normalizar el maestro: un registro por alpha code -> numeric code
currency_df = (
    currency_df
    .select(
        F.trim(F.col("currency_alphabetic_code")).alias("currency_alphabetic_code"),
        F.trim(F.col("currency_numeric_code").cast("string")).alias("currency_numeric_code"),
    )
    .dropDuplicates(["currency_alphabetic_code"])
)

# ---------------------------------------------------------------------------
# 2. Cruce: from_currency y to_currency contra el maestro (alpha -> numeric)
# ---------------------------------------------------------------------------
from_lookup = (
    currency_df
    .withColumnRenamed("currency_numeric_code", "from_currency_numeric_code")
    .withColumnRenamed("currency_alphabetic_code", "from_currency")
)

to_lookup = (
    currency_df
    .withColumnRenamed("currency_numeric_code", "to_currency_numeric_code")
    .withColumnRenamed("currency_alphabetic_code", "to_currency")
)

enriched_df = (
    exchange_rates_df
    .join(from_lookup, on="from_currency", how="left")
    .join(to_lookup, on="to_currency", how="left")
    .repartition(*PARTITION_COLS)
)

# ---------------------------------------------------------------------------
# 3. Purgar particiones a tocar + escribir y registrar en catálogo
# ---------------------------------------------------------------------------
if enriched_df.take(1):
    combos = [
        tuple(row) for row in
        enriched_df.select(*PARTITION_COLS).distinct().collect()
    ]

    for brand, exchange_date in combos:
        partition_path = (
            f"{TARGET_S3_PATH.rstrip('/')}/brand={brand}/"
            f"exchange_date={exchange_date}/"
        )
        glueContext.purge_s3_path(partition_path, options={"retentionPeriod": 0})

    enriched_dyf = DynamicFrame.fromDF(enriched_df, glueContext, "enriched_dyf")

    sink = glueContext.getSink(
        path=TARGET_S3_PATH,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=PARTITION_COLS,
        enableUpdateCatalog=True,
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=TARGET_TABLE)
    sink.writeFrame(enriched_dyf)
else:
    print("Sin particiones nuevas o modificadas (bookmark al día); nada que escribir.")

job.commit()
