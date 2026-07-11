# Ejemplo real — Glue job (`glue/scripts/mastercard/calculate/calculate.py`)

## Encabezado de módulo — ANTES

El header original era un bloque de comentarios `#`, no un docstring:

```python
# =============================================================================
# calculate.py — AWS Glue Job: Mastercard IPM Calculate
# =============================================================================
# Glue Job: itl-0004-itx-dev-intchg-02-glue-mc-calculate
# Glue 4.0 | Spark 3.3 | Python 3 | Worker G.1X x2
#
# Adapta mc_calculate.py para AWS Glue, reemplazando:
#   - SQLite  (Database)       →  DynamoDB  (boto3)
#   ...
#
# Job Parameters (siempre presentes):
#   --S3_REFERENCE         s3://itl-0004-itx-dev-intchg-02-s3-reference
#   --S3_STAGING           s3://itl-0004-itx-dev-intchg-02-s3-staging
#
# Job Parameters (pasados por el orquestador en cada ejecución):
#   --client_id            ID del cliente  (ej: "CLIENT01")
#   ...
#
# Estructura S3 esperada:
#   [S3_REFERENCE]/country/data.parquet
#   ...
```

## Encabezado de módulo — DESPUÉS

Se convierte a docstring de módulo real (`"""..."""`), con el mismo título +
separador + `Archivo:` + `S3 Script:` que usan los Lambda handlers, pero
adaptando las secciones a lo que un Glue job necesita documentar
(`Job Parameters` y `Estructura S3 esperada` en vez de `Variables de
entorno`):

```python
"""
calculate.py — Job real: itl-0004-itx-dev-intchg-02-glue-mc-calculate
================================================================================
Archivo:     glue/scripts/mastercard/calculate/calculate.py
S3 Script:   s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/mastercard/calculate.py

Job de Glue (PySpark, Glue 4.0, Worker G.1X x2) que calcula, para cada
transacción Mastercard (MTI 1240/1442), los campos derivados necesarios
para asignarle una tarifa de interchange: jurisdicción (on-us / off-us /
intraregional / interregional) vía cruce por rango de PAN contra la tabla
IAR (Interchange Assessment Rules), tipos de cambio hacia la moneda de
liquidación y local del cliente, el monto y moneda del reporte de
liquidación, y el flag de exclusión de mensajes de fee collection (MTI
1644, function code 691) ya cubiertos por otro registro.

Funciones principales:
  build_lookup_691_spark() + apply_exclude_flag()  → exclusión de duplicados fee-collection

Job Parameters (siempre presentes):
  --S3_REFERENCE         s3://itl-0004-itx-dev-intchg-02-s3-reference
  --S3_STAGING           s3://itl-0004-itx-dev-intchg-02-s3-staging

Job Parameters (pasados por el orquestador en cada ejecución):
  --client_id            ID del cliente  (ej: "CLIENT01")
  --file_id              ID del archivo  (ej: "ABC123XYZ...")
  --file_type            IN | OUT
  --file_date            YYYY-MM-DD  (fecha del archivo, para IAR y exchange_rate)
  --outputs              JSON: [{"mti":"1240","s3_key":"staging/…"}, …]
  --dynamodb_table_client  tabla DynamoDB de clientes
  --s3_key_1644_cln      path en staging del folder 400_IPM_1644_CLN  (para lookup 691)

Estructura S3 esperada:
  [S3_REFERENCE]/country/data.parquet
  [S3_REFERENCE]/region/data.parquet
  [S3_REFERENCE]/currency/data.parquet
  [S3_REFERENCE]/mastercard_brand_product/data.parquet
  [S3_REFERENCE]/mastercard_iar/historic_data.parquet   ← PROVISIONAL
  [S3_REFERENCE]/exchange-rates-glue/brand=Mastercard/exchange_date=YYYY-MM-DD/*.parquet
  [S3_STAGING]/{s3_key_input}/…_1240.parquet            ← CLN input
  [S3_STAGING]/{s3_key_output}/…_1240.parquet           ← CAL output
"""
```

Para jobs de Glue más simples (una sola tabla de entrada/salida, sin
parámetros de orquestador), la sección larga de `Job Parameters` +
`Estructura S3 esperada` se reemplaza por algo más compacto tipo
`Database:` / `Input:` / `Output:` — ver `glue/scripts/reports/exchange_rates/format_exchange_rates.py`
ya documentado como referencia corta.

## Docstring de función — ANTES

```python
def build_cln_schema_from_dynamodb(dynamo_table_fields: str, mti: str) -> StructType:
    """
    Consulta la tabla DynamoDB de campos Mastercard y construye el StructType
    para leer los parquets CLN del MTI indicado.
    - Los campos de metadatos del pipeline van hardcodeados al inicio.
    - 'date_and_time_local_transaction_de_12' siempre se mapea a LongType.
    - El campo 'date' (partición Hive) se añade al final hardcodeado.
    """
```

## Docstring de función — DESPUÉS

```python
def build_cln_schema_from_dynamodb(dynamo_table_fields: str, mti: str) -> StructType:
    """
    Construye el StructType de PySpark usado para leer los Parquets CLN de un
    MTI determinado, a partir de la definición de campos almacenada en la
    tabla DynamoDB `mastercard_fields`.

    Los campos de metadatos del pipeline (file_id, ref_id, etc.) se agregan
    hardcodeados al inicio porque no viven en DynamoDB, y el campo 'date'
    (partición Hive) se agrega al final. 'date_and_time_local_transaction_de_12'
    siempre se mapea a LongType (ver `_TIMESTAMP_NS_AS_LONG`).

    Args:
        dynamo_table_fields: nombre de la tabla DynamoDB (ej: itl-0004-itx-dev-dynamo-mastercard_fields-02).
        mti: MTI transaccional a construir, '1240' o '1442'.

    Returns:
        StructType listo para pasar a `spark.read.schema(...)`.

    Ejemplo:
        schema = build_cln_schema_from_dynamodb("itl-0004-...-mastercard_fields-02", "1240")
    """
```

## Módulo interno (no entry-point) — patrón simplificado

Los módulos importados por un handler (no desplegados directamente como
Lambda/Glue job — ej. `ardef/calculate.py`, `iar/extract.py`,
`persistence/database.py`) NO llevan el título largo con "Lambda real:"/
"Job real:" ni "Archivo:"/"S3 Script:". Usan solo el nombre de archivo como
título:

```python
"""
calculate.py

Cuarta etapa del pipeline ARDEF. Calcula la vigencia (valid_until) de cada
registro de rango de BIN/regla contra la tabla maestra acumulada lu_ardef
(en la capa REFERENCE), soportando llegada de registros fuera de orden, y
mantiene esa tabla maestra actualizada en S3 para que las Lambdas de
calculate de Visa (calc_ardef) puedan cruzar transacciones contra el rango
vigente en la fecha correspondiente.
"""
```

Sus funciones y clases igual llevan el mismo tratamiento completo
(`Args:`/`Returns:`/`Ejemplo:`) — lo único que cambia es el encabezado de
módulo, no el nivel de detalle interno.

## Apuntar a un comentario `#` en vez de duplicarlo (`glue/scripts/visa/interchange/interchange.py`)

Cuando el código ya tiene un comentario `#` que explica una decisión no
obvia con suficiente detalle (una fórmula, una dirección de join), el
docstring de la función NO debe repetir ese texto — debe apuntar a él. Así
queda una sola fuente de verdad y no hay que sincronizar dos lugares si
cambia la razón en el futuro.

```python
def calculate_fee_amounts(df: DataFrame, rates_pd: pd.DataFrame) -> DataFrame:
    """
    Calcula `interchange_fee_amount` en Spark a partir de los campos que
    `_evaluate_rules_pandas` ya asignó a cada transacción, con la fórmula
    `fee_variable × source_amount + fee_fixed_convertido`, donde
    `fee_min`/`fee_cap` actúan como piso/techo del resultado. El detalle de
    en qué moneda queda expresado el fee y por qué el join de tasas va en
    esa dirección está documentado en el comentario inline justo antes del
    join, más abajo (decisión ya validada, ver decisions.md — "dirección
    del exchange_value").
    ...
    """
    ...
    # Join direction: (from=fee_ccy, to=source_ccy) → exchange_value = rate(fee_ccy → source_ccy)
    # Converts fee_fixed/fee_min/fee_cap from fee_ccy to source_ccy.
    # fee = fee_variable × source_amount_src_ccy + fee_fixed_fee_ccy × rate(fee_ccy→src_ccy)
    df = df.join(...)
```

## Borrar un comentario que quedó duplicado por el docstring

Caso inverso: si al escribir el docstring terminás reformulando un
comentario existente casi palabra por palabra, borrar el comentario en vez
de dejar la misma explicación en dos lugares. Ejemplo real (`process_output`,
mismo archivo) — ANTES:

```python
    log_info("  Joining CLN + CAL...")
    # CAL columns (ARDEF-derived computed fields) take precedence over CLN raw Visa fields.
    # Rename overlapping CLN columns with _cln suffix so no data is lost and CAL version is used.
    key_cols = {"record", "content_hash"}
```

DESPUÉS (el docstring de `process_output` ya dice: "une CLN+CAL por `record`
— las columnas de CAL, derivadas de ARDEF, tienen prioridad sobre las de CLN
cuando hay solapamiento — las de CLN se renombran con sufijo `_cln`..."):

```python
    log_info("  Joining CLN + CAL...")
    key_cols = {"record", "content_hash"}
```

Mismo criterio se aplicó a los comentarios `✅ Preservar índice original...`/
`✅ Recuperar filas originales...` en `_apply_amount_currency` (duplicaban la
nota de implementación que pasó al docstring) y a
`#Yield SOLO las columnas esenciales...` en `process_pandas_partitions`
(duplicaba la última oración del docstring de esa función).

Lo que NO se borró en el mismo archivo: los labels `# OPT 1: ...`,
`# OPT 2: ...`, `# OPT 3: Early exit` de `_evaluate_rules_pandas` — son
etiquetas cortas de sección, no párrafos de razonamiento duplicado.
