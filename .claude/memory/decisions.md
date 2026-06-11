# Decisiones de arquitectura

Decisiones no obvias tomadas durante el desarrollo. Cada entrada explica el **qué**, el **por qué** y las **alternativas descartadas**.

---

## Por qué ARDEF e IAR no usan Step Functions

**Decisión:** `lmbd-vi-ardef` y `lmbd-mc-iar` se invocan directamente desde el router (async), sin pasar por Step Functions.

**Razón:** Son archivos de reglas y rangos de BINes — procesos relativamente livianos y autocontenidos. No requieren las múltiples etapas pesadas del flujo transaccional.

**Alternativa descartada:** Crear un Step Function propio para ARDEF/IAR. Se descartó porque añade complejidad operacional sin beneficio real dado el tamaño del procesamiento.

---

## Por qué Glue y no Lambda para Calculate e Interchange

**Decisión:** `glue-vi-calculate` y `glue-vi-interchange` son Glue jobs (PySpark), no Lambdas.

**Razón:** 
- La lógica de cálculo de fees requiere joins complejos y operaciones sobre millones de registros simultáneamente.
- PySpark en Glue permite procesamiento distribuido que no cabe en el modelo de memoria/timeout de Lambda (máx 10240 MB / 900s).
- El job de interchange contrasta la tarificación propia contra los registros VSS (Data Quality) — operación que requiere tener ambos conjuntos de datos en memoria al mismo tiempo.

**Glue config:** Calculate: G.1X × 2 workers. Interchange: G.2X × 4 workers. Glue 4.0.

---

## Por qué el diseño es configuration-driven (DynamoDB)

**Decisión:** La lógica de campos, validaciones y patrones de archivo vive en DynamoDB, no hardcodeada en el código.

**Razón:** Visa y Mastercard actualizan sus especificaciones periódicamente. Tener la definición de campos en DynamoDB permite ajustar sin redesplegar Lambdas.

**Tablas involucradas:**
- `itx-file-pattern` → qué tipo de archivo es cada uno (regex por prioridad)
- `itx-visa-fields` → definición de campos por tipo de registro (~430 items)
- `itx-client` → configuración por cliente (encoding MC, etc.)

---

## Por qué chunked processing en Lambdas

**Decisión:** Las Lambdas de procesamiento (transform, extract, clean) dividen los archivos en chunks en vez de cargarlos completos en memoria.

**Razón:** Los archivos interchange pueden superar 1.5 GB. Cargarlos completos en memoria superaría los límites de Lambda incluso con 10240 MB, además de aumentar el riesgo de timeout.

**Parámetros actuales:**
- `transform`: chunks de 128 MB, flush cada 1,000,000 records
- `extract` / `clean`: chunks de 300,000 filas

---

## Por qué el router re-dispara a sí mismo con ZIPs

**Decisión:** Cuando el router detecta un ZIP, invoca `lmbd-unzip` de forma async (sin esperar), y cada archivo extraído se sube de vuelta al landing bucket, lo que genera nuevos S3 events que vuelven a disparar el router.

**Razón:** Paralelismo gratis. Si un ZIP contiene 5 archivos, los 5 se procesan en paralelo sin necesidad de orquestación adicional. El router no necesita saber que viene de un ZIP.

---

## Por qué Mastercard tiene un paso "Interpreter" que Visa no tiene

**Decisión:** El flujo Mastercard tiene `lmbd-mc-interpreter` como primer paso, antes del transform.

**Razón:** Los archivos IPM de Mastercard son binarios con estructura ISO-8583 (MTI + bitmaps + Data Elements), muy diferente al texto plano de ancho fijo de Visa. El interpreter traduce este formato a Parquets estructurados por MTI, que el transform puede procesar con la misma lógica que Visa.

**Complejidades adicionales del interpreter:**
- Archivos pueden venir "bloqueados" en bloques de 1014 bytes (requiere `unblock_1014`)
- Encoding configurable por cliente: `latin-1` o `cp500` (EBCDIC), definido en DynamoDB tabla `client`
- Mensajes delimitados por RDW (4 bytes big-endian)

---

## Por qué mc-interchange NO contrasta contra MTI 1644 (a diferencia de Visa vs VSS)

**Decisión:** `glue-mc-interchange` solo procesa MTIs 1240 y 1442 (transaccionales). No realiza Data Quality contra MTI 1644 (mensajes de liquidación MC).

**Razón:** El scope actual del interchange MC es la asignación de tarifas IAR a las transacciones. El contraste DQ contra los registros de liquidación 1644 es una funcionalidad adicional que puede incorporarse en una iteración posterior, cuando el pipeline transaccional esté completamente validado.

**Diferencia con Visa:** `glue-vi-interchange` sí contrasta contra registros VSS (TC 46) como parte del mismo job. En MC, el MTI 1644 existe en la capa CLN pero no se usa en el interchange actual.

**Inputs del interchange MC:** CLN (`400_IPM_{mti}_CLN`) + CAL (`500_IPM_{mti}_CAL`) + datos de referencia S3 (`currency/`, `exchange_rate/`, `mc_rules/`). Output: `600_IPM_{mti}_ITX`.

---

## Por qué mc-store fusiona CLN + CAL + ITX por llave (file_id, file_idn, ref_id) y no por posición (axis=1)

**Decisión (actualizada 2026-06-11):** `lmbd-mc-store` (`_store_output`) ya NO usa `pd.concat(frames, axis=1)` por índice posicional. Ahora fusiona CLN + CAL + ITX con `df.merge(..., on=KEYS, how="left", validate="one_to_one")`, donde `KEYS = ["file_id", "file_idn", "ref_id"]`. Antes de cada merge:
- `_normalize_merge_keys(df, KEYS)` castea las columnas llave a `string` nullable y aplica `.str.strip()` (evita mismatches por espacios o tipos mixtos int/str).
- Se valida que las 3 columnas llave existan en CLN/CAL/ITX (`raise ValueError` si falta alguna).
- Se valida que no haya duplicados por `KEYS` en ninguno de los tres frames (`raise ValueError` si `df.duplicated(subset=KEYS).sum() > 0`).
- Solo se agregan al merge las columnas de CAL/ITX que no existen ya en `merged` (mismo principio que la versión anterior: "solo columnas nuevas").
- CAL e ITX siguen siendo opcionales: `S3.exceptions.NoSuchKey` (o cualquier excepción de lectura/merge) se loguea como warning y el merge continúa solo con lo que ya se tiene — igual que antes.

**Decisión anterior (superada, documentada por completo):** `pd.concat(frames, axis=1)` — merge horizontal por índice posicional, asumiendo que CLN/CAL/ITX para el mismo MTI y archivo tienen exactamente el mismo número de filas en el mismo orden.

**Razón del cambio:** La garantía de orden posicional entre etapas (CLN/CAL/ITX) resultó frágil en la práctica — `glue-mc-calculate` y `glue-mc-interchange` corren en Spark, donde el orden de las filas de salida no está garantizado entre ejecuciones aunque el contenido sea el mismo (shuffles, particionamiento). Un join por llave es la forma correcta de garantizar que cada fila de CAL/ITX se una con la fila CLN correcta, independientemente del orden físico de cada Parquet. `ref_id` se agregó como tercera columna de `glue-mc-interchange` (`run_interchange_mti`, ver más abajo) precisamente porque `file_id + file_idn` por sí solos no son suficientes para identificar de forma única cada registro dentro de un mismo archivo/MTI — `ref_id` es el identificador de fila a nivel de mensaje IPM.

**Por qué falla rápido (`raise ValueError`) en vez de degradar:** Si las llaves no son únicas o no existen, un `how="left"` silencioso produciría duplicación de filas (fan-out) o nulls masivos en las columnas nuevas — un bug de este tipo es mucho más difícil de detectar en `operational` (Athena) que en el log del Lambda. Las validaciones convierten un dato incorrecto silencioso en un error explícito en CloudWatch.

**Alternativa descartada:** Mantener el merge posicional y solo "arreglar" el orden en Spark con `orderBy` antes de escribir CAL/ITX. Descartado porque requeriría garantizar el mismo `orderBy` determinístico en CAL e ITX (etapas independientes, con sus propios shuffles/joins) — más frágil y más difícil de auditar que validar llaves explícitas.

**Impacto en `glue-mc-interchange`:** `run_interchange_mti()` ahora incluye `F.col("ref_id")` en la selección de columnas de salida (antes no estaba) — es requisito para que mc-store pueda usarlo como llave de merge.

---

## Por qué el router extrae la fecha MC desde el trailer 695 en chunks (sin descarga completa)

**Decisión:** `extraer_fecha_mc()` en el router lee el archivo IPM en chunks de 8 MB buscando el primer trailer MTI 1644 / FC 695, extrae el PDS tag "0105" (file_idn) y deriva la fecha YYMMDD → YYYY-MM-DD. No descarga el archivo completo.

**Razón:** Consistencia con el patrón ya usado para archivos Visa (solo los primeros 50 bytes del header). Los archivos MC pueden superar 1.5 GB — descargarlos completos en el router para extraer una fecha sería prohibitivo en costo y tiempo. El trailer 695 con la fecha suele estar en los primeros pocos MB del archivo.

**Detalles de implementación:**
- Archivos bloqueados (`file_block=True`): chunks alineados a múltiplos de 1014 bytes + `_mc_unblock_chunk` antes de parsear
- Overlap de 8 KB entre chunks para no cortar mensajes en el límite de chunk
- Guardia `MAX_CHUNKS=100` (~800 MB máximo antes de retornar `datetime.utcnow()` como fallback
- Path de extracción: `DE48 del mensaje 695 → PDS tag "0105" → file_idn[3:9] → YYMMDD`

---

## Por qué los Glue jobs tienen args.json en el repositorio

**Decisión:** Cada Glue job tiene un `args.json` junto a su script con los `DefaultArguments` usados en AWS.

**Razón:** Permite reproducir exactamente la configuración del job (buckets, Spark conf, logging) desde el repositorio sin tener que consultar la consola AWS. También sirve como documentación de los argumentos que el Step Function debe pasar al invocar el job.

**Contenido típico:** rutas S3 de staging y reference, configuración de Spark (rolling logs, event logs), habilitación de métricas y job insights CloudWatch.

---

## Por qué el reporte usa Glue PySpark y no Athena ni el SP original en PostgreSQL

**Decisión:** El reporte de transacciones (`glue-vi-mc-reporting`) se implementa como Glue job PySpark, no como consulta Athena ni manteniéndolo en PostgreSQL.

**Razón:**
- El SP original (`analytics.generate_transaction_tables`) tardaba hasta 30 minutos para el cliente más grande, iterando fecha por fecha en un loop PL/pgSQL. PySpark lee el rango completo con partition pruning en una sola operación vectorizada.
- La lógica requiere joins contra múltiples tablas de referencia (country, exchange rates, BIN products) y lógica condicional por cliente (`duplicate_on_us`, `scheme_fee`) que supera lo que Athena puede expresar cómodamente en SQL estático.
- Elimina la dependencia de una base de datos PostgreSQL como capa intermedia — todo el reporte vive en S3.

**Athena descartada para este caso:** útil para consultas ad-hoc sobre los datos ya generados, no para generarlos. El reporte implica joins y transformaciones que en Athena requerirían CTEs complejos sin control de rendimiento.

---

## Por qué el reporting job lee directo desde S3 y no vía Glue catalog

**Decisión:** `glue-vi-mc-reporting` lee los Parquets de `s3-operational` usando `spark.read.parquet(path)` con filtros de partición, sin pasar por tablas del Glue catalog.

**Razón:** La estructura de paths `{client_id}/{brand}/{data_type}/file_type=*/date=*/` tiene el `client_id` como primer nivel del path, no como partición Hive. El crawler crearía una tabla separada por combinación `{client_id}/{brand}/{data_type}` — inmanejable para reportería multi-cliente. Leer directo desde S3 con path parametrizado por `client_id` es más simple y eficiente.

**Los crawlers siguen teniendo valor** para Athena (consultas ad-hoc sobre un cliente específico). Para el job de reportería, el path parametrizado es suficiente.

---

## Por qué se agrega content_hash como primera columna en cada Parquet del pipeline Visa

**Decisión:** Desde transform hasta interchange, cada Parquet del pipeline Visa incluye `content_hash` como primera columna.

**Razón:** Sin esta columna no hay forma de saber qué archivo originó cada fila al consultar las tablas en Athena. El `content_hash` es el MD5 del archivo fuente y ya se pasa como parámetro en todos los steps del Step Function — agregarlo al Parquet no tiene costo adicional.

**Implementación:**
- `lmbd-vi-transform`: `ParquetBatchWriter` recibe `content_hash` como parámetro; es el primer campo en el schema PyArrow
- `lmbd-vi-extract`: `extracted_chunk.insert(0, 'content_hash', content_hash)` tras cada batch
- `lmbd-vi-clean`: sin cambio — `_clean_chunk` itera todas las columnas del input y pasa `content_hash` automáticamente
- `glue-vi-calculate`: `"content_hash"` al inicio de `output_columns` en las 3 funciones (BASEII, SMS, VSS)
- `glue-vi-interchange`: `"content_hash"` al inicio de `interchange_cols`
- `lmbd-vi-store`: sin cambio — el merge hereda todas las columnas de CLN

**Alternativa descartada:** Usar `input_file_name()` de Spark en Athena/Glue para derivar el hash del path. Descartado porque los Lambdas (transform, extract, clean) usan pandas/PyArrow, no Spark, y los Glue jobs requerirían expresiones adicionales en cada consulta.

---

## Por qué se agrega content_hash en el pipeline Mastercard (interpreter → calculate)

**Decisión (sync 2026-06-11):** Replicando el patrón ya validado en Visa (ver decisión anterior), el pipeline Mastercard ahora propaga `content_hash` (MD5 del archivo fuente) a través de todas sus etapas: `lmbd-mc-interpreter` → `lmbd-mc-transform` → `lmbd-mc-extract` → `lmbd-mc-clean` → `glue-mc-calculate`. Todas estas funciones ganaron un parámetro `content_hash: str = ""`.

**Razón:** Misma que Visa — sin esta columna no hay forma de saber qué archivo originó cada fila al consultar `operational` vía Athena, y el valor ya viaja en el payload del Step Function (`itx-mastercard-orchestrator`) sin costo adicional.

**Diferencia de implementación con Visa:** En Visa, `lmbd-vi-clean` propaga `content_hash` automáticamente porque itera todas las columnas del input (que ya lo trae desde transform). En Mastercard, `glue-mc-calculate` (`process_file`) lo agrega explícitamente al final con `df_final = df_final.withColumn("content_hash", F.lit(content_hash))` (Spark `lit`, no viene como columna del CLN) — porque calculate es el primer punto del pipeline MC donde se trabaja en Spark y es más simple inyectarlo ahí que propagarlo columna a columna desde el interpreter (pandas) hasta calculate.

**Caso particular — `glue-mc-interchange` recibe `--content_hash` pero no lo usa:** El ASL de `itx-mastercard-orchestrator` pasa `"--content_hash.$": "$.interchange_input.content_hash"` al job de interchange, pero `interchange.py` no declara ni usa ese argumento (Glue's `getResolvedOptions` simplemente lo ignora si no está en la lista de opciones requeridas). No es un bug — `content_hash` ya queda materializado en CAL (vía `glue-mc-calculate`) y mc-store lo hereda en el merge sin necesidad de que interchange lo reescriba. Si en el futuro `interchange.py` necesita `content_hash` para algo (logging, trazabilidad de su propio output), el argumento ya está disponible en el job sin cambios en el ASL.

---

## Por qué el reporting job ejecuta un cliente por vez (no lista de clientes)

**Decisión:** `glue-vi-mc-reporting` (archivo: `glue/scripts/reports/get_transaction/get_transaction.py`) procesa un único cliente por ejecución. El parámetro es `--client_code` (singular).

**Razón:** Simplifica el job y lo hace más predecible en tiempo y memoria. Step Functions puede invocar múltiples ejecuciones en paralelo si se necesitan varios clientes. Los parámetros del job están dentro de `main()` con `global` declarations para las variables usadas por las funciones auxiliares (`OPERATIONAL_BUCKET`, `BUCKET_REF`, `DDB_CLIENT_TABLE`, `TABLE_SUFFIX`).

**Alternativa descartada:** `--client_codes` separados por coma con loop interno. Descartado porque mezcla tiempos de ejecución de clientes distintos en un solo job, complica el retry en caso de fallo parcial, y no aprovecha el paralelismo nativo de Step Functions.

---

## Por qué el reporting job no necesita el join M4 (m_interchange_rules_visa)

**Decisión:** `glue-vi-mc-reporting` no hace join contra una tabla de reglas de interchange para obtener `interchange_rule` (equivalente a `fee_descriptor` en el SP). Lee la columna directamente del Parquet operational.

**Razón:** El SP original hacía `JOIN m_interchange_rules_visa M4 ON M4.intelica_id = T3.intelica_id` para obtener `M4.fee_descriptor`. En el nuevo pipeline, `glue-vi-interchange` ya calcula y escribe `interchange_fee_descriptor` en la capa ITX, que `lmbd-vi-store` consolida en el Parquet final de operational. La columna ya está materializada — el join es redundante.

**Impacto:** Un join menos sobre una tabla de referencia potencialmente grande. El campo `interchange_fee_descriptor` en el Parquet operational es la fuente de verdad.

---

## Por qué lmbd-vi-store lee el CAL con _read_parquet_arrow en vez de _read_parquet_from_s3

**Decisión:** En `store_output` (`lambdas/visa/store/src/handler.py`), el Parquet CAL se lee con `_read_parquet_arrow()` (devuelve `pa.Table`) y se extrae el schema de columnas antes de convertir a pandas, para poder restaurar tipos degradados por el round-trip pandas/pyarrow.

**Razón:** El round-trip por pandas degrada dos tipos de columnas del CAL:
- `INT64+nulls → float64` (numpy no tiene int nullable). Si el CAL tiene columnas enteras con nulls (como `timeliness`), al reconstruir la Arrow Table con `pa.Table.from_pandas(merged)`, PyArrow infiere `double`. El crawler de Glue luego detecta esas columnas como `double` en la capa operational en lugar de `bigint`.
- `string 100% null → NullType`. Si una columna del CAL es 100% null en un archivo concreto (p.ej. `message_reason_code`, `type_of_purchase` para ciertos `file_id`), pandas la representa como `object` con puros `None`, y `pa.Table.from_pandas()` no puede inferir el tipo real → le asigna `pa.null()` (NullType, se escribe como `INT32` en Parquet). Ver gotcha "lmbd-vi-store: columnas NullType en operational rompen lectura de directorio completo con Spark" para el impacto downstream.

**Solución (generalizada 2026-06-10):** Leer CAL como Arrow Table, capturar `_cal_dtype_map = {nombre: tipo Arrow}` para TODAS las columnas, convertir a pandas normalmente, y después de cada `pa.Table.from_pandas(merged)` restaurar el tipo original con `merged_table.set_column(..., col.cast(atype))` cuando el tipo actual sea `NullType` o cuando sea `float64` y el original era entero. Arrow castea tanto `NullType → string/lo-que-sea` como `float64 null → int64 null` sin pérdida de datos.

**Alternativa descartada:** `table.to_pandas(use_nullable_dtypes=True)` — más limpio, pero requiere PyArrow ≥ 2.0 y el layer usa una versión anterior.

---

## Por qué product_program_id en glue-vi-mc-reporting usa una nueva tabla `visa_bin_products` en s3-reference (join M5)

**Decisión (2026-06-11):** `product_program_id` (antes `NULL` fijo, TODO documentado) se calcula ahora con un join M5: `product_id` (ya calculado en `glue-vi-calculate` via cruce ARDEF, presente en CLN/operational) → `bin_product_id` en `s3://itl-0004-itx-dev-intchg-02-s3-reference/visa_bin_products/data.parquet` → `range_program_id`.

**Origen de la tabla:** export CSV de la tabla maestra `m_visa_bin_products` de PostgreSQL legacy (58 filas: `bin_product_id, short_description, bin_card_type, range_program_id, app_creation_date, app_creation_user`), provisto por el usuario y convertido 1:1 a Parquet (sin transformación) en `visa_bin_products/data.parquet`.

**Implementación en `get_transaction.py`:**
- `load_visa_bin_products()` — nueva función junto a `load_country()`/`load_exchange_rates()`, selecciona solo `bin_product_id, range_program_id`.
- `transform_visa_baseii()` y `transform_visa_sms()` ahora reciben `bin_products_df` como parámetro; join M5 análogo a M1/M2 (country): `df["product_id"] == bin_products_df["bin_product_id"]`, left join, alias `product_program_id_m5`.
- `process_client_range()` y `main()` propagan `bin_products_df` (cargado y cacheado una vez, igual que `country_df`).
- MC (`product_program_id` en `transform_mastercard`) queda como TODO separado — requiere `m_mastercard_bin_products`, tabla distinta no provista aún.

**Validación (2026-06-11):** Re-run `glue-test-1` (`jr_f374a87d3849a2d8f4fa1c762c9ece25a4ba51b21118f4233460476253d73f65`, `report_suffix=20260105_tst3`, EBGR 2026-01-01..2026-01-05) → SUCCEEDED. `product_program_id`: 0 nulls (antes 100% null), suma=57,849,742=legacy, value_counts idénticos (103→555,220, 102→6,491), mapeo `product_code→product_program_id` idéntico (E,F,N,P→103; G→102) en ambos sistemas. **Resuelve completamente este TODO.**

**Alternativa descartada:** Calcular `product_program_id` directamente en `glue-vi-calculate` (como los otros campos ARDEF). Descartado porque `product_program_id` es un atributo del *producto* (tabla pequeña, 58 filas, cambia raramente) no de la transacción — más simple resolverlo en el reporting job via join liviano que mantenerlo sincronizado en cada `calculate.parquet`.

---

## Por qué glue-vi-mc-reporting (glue-test-1) lee `exchange_rate/rate_date=YYYY-MM-DD/` y no `exchange-rates/brand={brand}/exchange_date=YYYY-MM-DD/`

**Decisión:** `load_exchange_rates()` en `glue/scripts/reports/get_transaction/get_transaction.py` lee `s3://itl-0004-itx-dev-intchg-02-s3-reference/exchange_rate/rate_date=YYYY-MM-DD/`, filtra por columna `brand` (`'VISA'` / `'MasterCard'`, comparación case-insensitive) y renombra columnas a `exchange_date, from_currency, to_currency, fx_rate`.

**Razón:** Existen dos ubicaciones de tipo de cambio en `s3-reference`:
- `exchange-rates/brand={Visa,MasterCard}/exchange_date=YYYY-MM-DD/` — cobertura incompleta (no tiene todos los pares de moneda/fechas necesarios, ej. Visa EUR→USD para algunas fechas) y además sus columnas reales (`currency_from, currency_to, currency_from_code, currency_to_code, exchange_value`) no coinciden con las que el código asumía (`from_currency, to_currency, fx_rate`).
- `exchange_rate/rate_date=YYYY-MM-DD/` — cubre 2025-12-01..2026-04-30, ambas marcas en una sola tabla distinguidas por la columna `brand`. Es la fuente que ya usan otros procesos (interchange) y que sí tiene los pares de moneda necesarios.

El bug original (`Column 'to_currency' does not exist`) solo se manifestó el 2026-06-10 porque hasta entonces el job fallaba ANTES (por el `SchemaColumnConvertNotSupportedException` de columnas NullType en `lmbd-vi-store`, ya resuelto) — nunca había llegado a ejecutar `_join_exchange_rates()`.

**Pendiente:** hay un nuevo método de extracción de tipo de cambio Visa en desarrollo (mencionado por el usuario 2026-06-10). Cuando esté disponible, revisar si `load_exchange_rates()` debe apuntar a esa nueva fuente en vez de (o además de) `exchange_rate/`.

**Alternativa descartada:** mantener `exchange-rates/brand={brand}/` y solo corregir los nombres de columna — descartado porque esa tabla no tiene cobertura completa de pares de moneda/fechas (`exchange-rates/brand=Visa/` no tenía EUR→USD para `exchange_date=2026-01-01`, mientras que `exchange_rate/rate_date=2026-01-05/` sí).

---

## Por qué se agregaron business_transaction_cycle y settlement_report_currency_code en glue-vi-calculate

**Decisión (2026-06-11):** Dos nuevos campos calculados en `glue/scripts/visa/calculate/calculate.py`, BASEII pasa de 28 a 30 campos, SMS de 26 a 27.

**`business_transaction_cycle`** (BASEII/draft, `calc_business_transaction_cycle_draft`):
- Deriva de `draft_code` (transaction_code) + `usage_code`, replicando la clasificación del SP legacy:
  - `draft_code in (05,06,07)` (purchase): `usage=1→11`, `usage=2→23`, `usage=9→6`, otro→255
  - `draft_code in (15,16,17,35,36,37)` (reversal): `usage=1→1`, `usage=9→4`, otro→255
  - `draft_code in (25,26,27)` (chargeback): `usage=1→11`, `usage=9→6`, `usage=2→25`, otro→255
  - cualquier otro `draft_code` → 255
- SMS (`calc_business_transaction_cycle_sms`): la lógica es exclusiva de transaction_code BASEII — para SMS el campo se mantiene `NULL` (igual que en legacy), por eso existe la función dedicada que solo hace `F.lit(None).cast(IntegerType())`.

**`settlement_report_currency_code`** (solo BASEII/draft, `calc_settlement_report_currency_code_draft`):
- Si `calc_jurisdiction in (on-us, off-us)` y `settlement_flag != 0` → `local_currency_code` del cliente (tabla `client-02`, ya cargado en `client_data` vía `get_client_data()` — el mismo mecanismo que ya usaba `--dynamodb_table_client`).
- Caso contrario → `settlement_currency_code` del cliente.
- No existe equivalente para SMS — el campo no se agrega a `calculate_sms_fields`.

**Razón:** ambos campos son requeridos por el reporte (`glue-vi-mc-reporting` / `get_transaction.py`) y no estaban materializados en `calculate.parquet`; agregarlos en calculate evita que el reporting tenga que recalcularlos con su propia lógica duplicada.

**Validación (2026-06-11):** contra `93BF199C85D2DF243AFDABEE5572E8C0` (EBGR, 2026-01-03, 269,725 filas BASEII): `business_transaction_cycle` int32, 0 nulls, distribución por `draft_code`/`usage_code` coincide con la tabla de mapeo (ej. `draft_code∈{05,06,07,25}` + `usage_code=1` → `11`). `settlement_report_currency_code` string, 0 nulls, 100% `"EUR"` para EBGR (`local_currency_code=settlement_currency_code=EUR`).

**Reproceso masivo (2026-06-11):** ambos campos se reprocesaron para los 100 archivos EBGR/VISA/IN de enero 2026 (calculate + store) y se confirmaron en el catálogo Glue tras re-crawl (`business_transaction_cycle: int`, `settlement_report_currency_code: string` en `operational_ebgr_visa.baseii_drafts`). Detalle del reproceso masivo en `manual_execution.md` → "Sesión 2026-06-11".

---

## Por qué lmbd-mc-transform filtra list_parquet_files por prefijo file_id

**Decisión (sync 2026-06-11):** `list_parquet_files` en `lambdas/mastercard/transform/src/handler.py` ahora filtra los Parquets RAW del interpreter (`100_IPM_{MTI}_RAW/`) por `stem.upper().startswith(file_id.upper())` antes de procesarlos.

**Razón:** Mismo problema y misma solución ya documentados para `glue-mc-interchange` (ver gotcha "glue-mc-interchange: filtra por file_id para no reprocesar ejecuciones anteriores"). Sin el filtro, una re-ejecución de transform para un `file_id` listaría TODOS los Parquets RAW de la partición `file_type=X/date=YYYY-MM-DD` (incluyendo los de otros `file_id` ya procesados ese mismo día), reprocesándolos innecesariamente y potencialmente mezclando outputs de archivos distintos en el mismo `200_IPM_{MTI}_TRA/`.

**Alternativa descartada:** Ninguna — es la aplicación directa del patrón ya validado, no requirió evaluar alternativas.

---

## Por qué se agregó WaitForENIRelease (180s) entre Calculate e Interchange en itx-mastercard-orchestrator

**Decisión (sync 2026-06-11):** En `step-functions/mastercard/asl.json`, el estado `CheckCalculateResult` (Choice) ahora tiene como `Default` un nuevo estado `WaitForENIRelease` (Type=Wait, Seconds=180), que a su vez transiciona a `PrepareInterchangeInput` (el destino anterior de `Default`).

**Razón:** `glue-mc-calculate` corre con conexión a VPC (para acceder a recursos de red privados). Cuando un job Glue con conexión VPC termina, AWS tarda en liberar las ENIs (Elastic Network Interfaces) asociadas — si `glue-mc-interchange` se lanza inmediatamente después, puede fallar o quedarse bloqueado esperando ENIs disponibles (límite de ENIs por subnet/cuenta). El Wait de 180s da margen para que AWS complete la liberación antes de lanzar el siguiente job.

**Alternativa descartada:** Reintentos con backoff en `glue-mc-interchange` ante fallos de ENI. Descartado por ser más complejo de diagnosticar (el error de ENI no siempre es claro) y porque un Wait fijo es más simple y predecible para este caso conocido.

**Cambio relacionado — MaxConcurrentRuns 20→50:** En el mismo sync, `glue-mc-calculate` y `glue-mc-interchange` pasaron de `MaxConcurrentRuns=20` a `50` (igual que `glue-vi-calculate`/`glue-vi-interchange`), habilitando el mismo patrón de reproceso masivo paralelo (`tst_files/reprocessing/reprocess_vi_*.py`) para Mastercard cuando se necesite.

---

## Por qué lmbd-mc-exchange-rates se reescribió con scraping vía proxies (ProxyManager + orquestador/worker encadenado)

**Decisión (sync 2026-06-11):** `lambdas/mastercard/exchange-rates/src/handler.py` se reescribió casi por completo (+678 líneas) para obtener tipos de cambio Mastercard scrapeando `https://www.mastercard.com/marketingservices/public/mccom-services/currency-conversions/conversion-rates` (la API pública que alimenta el conversor de Mastercard), en vez de la fuente anterior.

**Componentes nuevos:**
- **`ProxyManager`** (clase thread-safe): pool de proxies con `pick()` round-robin, `report_failure()`/`report_success()` — banea un proxy tras `PROXY_BAN_AFTER=1` fallo consecutivo, y enmascara credenciales en los logs (`_mask_proxy_url`).
- **`validate_proxies()`**: al arrancar, prueba cada proxy con una consulta real (USD→EUR) y descarta los que no responden 200 — evita gastar tiempo en proxies muertos durante el scraping real.
- **Arquitectura orquestador/worker encadenada** (`mode="orchestrator"|"worker"` en el evento de invocación):
  - `run_orchestrator()`: genera el rango de fechas (`BEGIN_DATE`..`END_DATE`), carga todos los pares de moneda desde `resources/currencies.json`, los divide en `NUM_CHUNKS=10` chunks, borra los Parquets existentes de cada fecha (`delete_existing_parquets`), e invoca el primer worker (`chunk_index=0`) por fecha vía `invoke_next_worker()` (invocación async, `InvocationType="Event"`).
  - `run_worker()`: procesa su chunk con `ThreadPoolExecutor(MAX_WORKERS=9)` (cada thread = `process_sub_chunk`, con su propio proxy via `ProxyManager.pick()`), escribe el resultado a `s3://{S3_BUCKET}/{S3_PREFIX}/exchange_date={date}/`, e invoca el siguiente worker de la cadena (`chunk_index+1`) hasta agotar `chunks`.
- Esta arquitectura encadenada evita el timeout de un solo Lambda (900s máx) cuando el número de pares de moneda × fechas es grande — cada worker procesa solo 1 de 10 chunks y se auto-relanza.

**Razón:** Mastercard no expone una API/feed oficial de tipos de cambio históricos accesible para este proyecto; la única fuente disponible es la API pública del conversor web, que aplica rate-limiting/bloqueo por IP agresivo — de ahí la necesidad de rotar proxies y espaciar requests (`PAUSE_MIN/MAX` entre 1.0-1.3s por request).

**Nuevos archivos de recursos:**
- `lambdas/mastercard/exchange-rates/src/resources/currencies.json` (commiteado) — catálogo `{alphaCd, currNam}` usado para generar todos los pares `[src, dst]` con `src != dst`.
- `lambdas/mastercard/exchange-rates/src/resources/proxy_settings.json` (**NO commiteado** — agregado a `.gitignore` vía `**/resources/proxy_settings.json`) — contiene URLs de proxy con credenciales reales (`usuario:password@host:puerto`). Estructura: `proxy_settings.proxy_list_mastercard` (preferido) con fallback a `proxy_settings.proxy_list`, filtrando `status=="active"`.

**Cambios de configuración asociados:** `Timeout` 300s→750s (la cadena de workers necesita más margen por invocación), `MemorySize` 2048→512 MB (el trabajo es I/O-bound, no necesita memoria alta), `VpcConfig` removido (el scraping necesita salida a internet pública, no a recursos VPC privados), `env-vars.json` limpiado de la variable de prueba `testing_1`.

**Seguridad:** `proxy_settings.json` contiene credenciales reales de proxy (`Soporteintelica:JCbiJuhUpX` embebido en ~118 URLs). Verificar que el archivo nunca se commitee — si `git status` lo muestra como tracked/staged, hay que hacer `git rm --cached` antes de que la regla de `.gitignore` surta efecto (gitignore no afecta archivos ya trackeados).

**Alternativa descartada:** Mantener la fuente de tipos de cambio anterior (la que dejó la variable `testing_1` en `env-vars.json` como placeholder de pruebas). Descartado porque no cubría los pares de moneda/fechas necesarios para `glue-mc-calculate`/`glue-mc-interchange` — detalle de la fuente anterior no documentado, se reemplazó directamente.
