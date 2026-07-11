"""
format_exchange_rates.py — Job real: itl-0004-itx-dev-intchg-02-glue-exchange-rates
================================================================================
Archivo:     glue/scripts/reports/exchange_rates/format_exchange_rates.py
S3 Script:   s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/format_exchange_rates.py

Enriquece la tabla de tasas de cambio scrapeadas (exchange_rates, con solo
códigos alfabéticos de moneda) cruzándola contra el maestro de monedas
(m_currency) para resolver los códigos numéricos. Escribe el resultado
particionado por brand y exchange_date, con sobreescritura dinámica por
partición — solo reemplaza fechas presentes en la corrida actual, dejando
intactas las que no cambiaron.

Flujo:
  1. Lectura bookmarked de exchange_rates (solo cambios desde la última
     corrida exitosa) y lectura completa de m_currency (maestro pequeño que
     puede evolucionar).
  2. Normalización del maestro: un registro por código alfabético, deduplicado.
  3. Cruce left join por from_currency y to_currency hacia códigos numéricos.
  4. Reparticionamiento por brand/exchange_date + escritura en modo overwrite
     dinámico.
  5. Registración en catálogo Glue solo de particiones nuevas (las existentes
     conservan su ubicación S3, solo se actualiza el contenido del archivo).

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
"""

import sys

import boto3
from awsglue.context import GlueContext
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
REGION               = "eu-south-2"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# 1. Leer tablas fuente desde el Glue Data Catalog
#    exchange_rates: bookmarked, solo trae archivos nuevos o modificados
#    desde la última corrida exitosa de este job.
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
# 3. Escribir resultado: overwrite dinámico -> solo reemplaza las
#    particiones presentes en esta corrida (nuevas o recalculadas por un
#    archivo fuente corregido); el resto de particiones queda intacto.
# ---------------------------------------------------------------------------
if enriched_df.take(1):
    (
        enriched_df
        .write
        .mode("overwrite")
        .partitionBy(*PARTITION_COLS)
        .format("parquet")
        .option("compression", "snappy")
        .save(TARGET_S3_PATH)
    )

    # Registrar en el catálogo solo las particiones que aún no existen.
    # Las que ya estaban registradas conservan su ubicación S3 (no cambia
    # con el overwrite, solo cambia el contenido del archivo).
    combos = [
        tuple(row) for row in
        enriched_df.select(*PARTITION_COLS).distinct().collect()
    ]

    glue_client = boto3.client("glue", region_name=REGION)
    storage_descriptor = glue_client.get_table(
        DatabaseName=DATABASE, Name=TARGET_TABLE,
    )["Table"]["StorageDescriptor"]

    partitions_input = []
    for brand, exchange_date in combos:
        sd = dict(storage_descriptor)
        sd["Location"] = (
            f"{TARGET_S3_PATH.rstrip('/')}/brand={brand}/"
            f"exchange_date={exchange_date}/"
        )
        partitions_input.append({"Values": [brand, exchange_date], "StorageDescriptor": sd})

    for i in range(0, len(partitions_input), 100):
        batch = partitions_input[i:i + 100]
        response = glue_client.batch_create_partition(
            DatabaseName=DATABASE,
            TableName=TARGET_TABLE,
            PartitionInputList=batch,
        )
        errors = [
            e for e in response.get("Errors", [])
            if e["ErrorDetail"]["ErrorCode"] != "AlreadyExistsException"
        ]
        if errors:
            raise RuntimeError(f"Error registrando particiones: {errors}")
else:
    print("Sin particiones nuevas o modificadas (bookmark al día); nada que escribir.")

job.commit()
