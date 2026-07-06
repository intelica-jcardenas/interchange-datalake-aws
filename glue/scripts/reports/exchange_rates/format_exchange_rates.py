"""
Glue ETL Job: enriquecimiento de exchange_rates con el maestro de monedas (m_currency)

Database  : itl_0004_itx_dev_02_glue_database_exchange_rates
Input     : exchange_rates  (particionada por brand, exchange_date)
            m_currency_csv  (currency_numeric_code, currency_alphabetic_code)
Output    : exchange_rates_enriched
            Mismo particionado (brand, exchange_date) + from_currency_numeric_code
            y to_currency_numeric_code resueltos contra el maestro.

Requiere que ambas tablas de entrada ya estén catalogadas en la base de datos
indicada.

Job Bookmarks: habilitados solo para exchange_rates (--job-bookmark-option
job-bookmark-enable + transformation_ctx). m_currency_csv se relee completo
en cada corrida, ya que es un maestro pequeño que puede cambiar.

Escritura: overwrite dinámico por partición (spark.sql.sources.
partitionOverwriteMode=dynamic), así que si una fecha ya procesada se
sobrescribe en el origen, esta corrida vuelve a leerla (bookmark detecta el
archivo modificado) y reemplaza esa partición en destino en vez de
duplicarla. Las particiones no tocadas en la corrida actual no se modifican.
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
