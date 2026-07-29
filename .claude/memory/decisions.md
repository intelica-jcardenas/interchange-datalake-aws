# Decisiones de arquitectura

Decisiones no obvias tomadas durante el desarrollo. Cada entrada explica el **qué**, el **por qué** y las **alternativas descartadas**.

Las decisiones con implementación/validación extensas fueron resumidas aquí — el detalle completo (pasos de implementación, tablas de validación, reprocesos) está en `.claude/memory/decisions_archive.md` (no cargado automáticamente).

---

## Refresh de visa_rules desde excel V37 + 3 fixes en calculate.py/interchange.py — DESPLEGADO Y VALIDADO 2026-07-28

**Decisión:** `s3://itl-0004-itx-dev-intchg-02-s3-reference/visa_rules/data.parquet` fue reemplazado por el resultado de re-interpretar `VISA Reglas Intercambio V37.xlsx` (excel legacy más reciente) con una réplica local de `InterchangeRules.read_rules_visa()` (`tst_files/interchange_rules/build_and_compare_rules.py`). El `data.parquet` anterior (cargado 2026-05-18, nunca refrescado desde entonces) quedó respaldado en `s3://itl-0004-itx-dev-intchg-02-s3-reference/visa_rules_backup_pre_v37_20260728/data.parquet`.

**Por qué:** el excel V37 trae 2 columnas de condición nuevas (`settlement_flag`, `token_requestor_id`) respecto al schema anterior (79→81 columnas), más cambios de contenido (4,260 altas, 213 bajas/renumeraciones, ~2,480 reglas que pasan de `valid_until` abierto a una fecha de cierre real). El hallazgo más relevante: sin el refresh, el pipeline podía seguir aplicando reglas que Visa ya había dado de baja (tratadas como "vigentes hasta hoy" por el `fillna(date.today())` de `load_visa_rules()`).

**3 fixes aplicados en código antes de subir el parquet** (ver `.claude/memory/gotchas.md` para el detalle de investigación de cada uno):
1. `glue-vi-calculate`: nueva función `calc_token_requestor_id_draft()` — deriva `BLANK`/`VALID`/`INVALID` desde `token_requestor_id_sd`/`_sp` (raw fields ya existentes en `visa_fields`), replicando exacto la lógica de `adapters.py` (`load_visa_interchange`). Agregada a `calculate_baseii_fields()` y a su `output_columns`.
2. `glue-vi-interchange`: `settlement_flag`/`token_requestor_id` agregados a `drop_cols` de SMS en `_rename_rules()` — ninguno de los 2 existe como campo crudo para `type_record="sms"` en `visa_fields`, así que sin el drop el motor de reglas haría `KeyError` contra un batch SMS si una regla con esa condición activa cayera en esa jurisdicción.
3. `glue-vi-interchange`: **`cashback` removido de `CONDITIONS_TO_SKIP`** (bug preexistente, no introducido por V37 — la columna ya existía en el schema viejo) + nuevo `COLUMN_GROUP_YES_NO`/`_apply_yes_no()`, replicando el mecanismo real de `visa_interchange_rule_assign` (`adapters.py` línea ~5160: regla "No" → monto `==0`, "Yes"/cualquier otro valor → monto `>0`). Diferencia deliberada vs legacy: no se replica el truncamiento a entero de legacy (`.astype(int)`, que clasificaría mal un cashback real entre 0.01 y 0.99) — se compara el float directamente. También agregado a `drop_cols` de SMS (no existe como campo crudo para SMS).

**Fields explícitamente dejados sin implementar (decisión del usuario):** `message_identifier`/`validation_code` — tienen lógica real en `adapters.py` (`coalesce(t.message_identifier::text,'BLANK')`, etc.) pero el usuario los identificó marcados en el excel con un color que indica "no debe tomarse en cuenta todavía". Quedan en `CONDITIONS_TO_SKIP`, sin tocar.

**Validación antes de subir el parquet (orden crítico — código primero, luego datos):**
1. Desplegado `calculate.py`/`interchange.py` a S3 (`push-glue.ps1 -Group vi -Force`), confirmado byte a byte contra la versión local.
2. Smoke test contra las reglas VIEJAS (`glue-vi-calculate` + `glue-vi-interchange`, EBGR `93BF199C85D2DF243AFDABEE5572E8C0`/2026-01-03 y SBSA `7102B505635CCF3C8E8BE335DACA3193`/2026-01-27, encontrados vía Athena por jurisdicción `region_country_code='5'` Europa Intraregional y `region_country_code='ZA'`+`cashback>0` respectivamente) — SUCCEEDED en las 4 corridas, sin errores. Confirmó que el código nuevo no rompe nada contra datos reales antes de tocar la tabla de reglas.
3. Recién ahí: backup + subida de `visa_rules_new.parquet`, y re-corrida de `glue-vi-interchange` para los mismos 2 archivos — SUCCEEDED ambos (EBGR 484s, SBSA 374s). EBGR: 269,695/269,725 filas matchearon una regla (99.99%), tasa sana.

**Limitación conocida de la validación (aceptada por el usuario):** ninguna transacción de los 2 archivos de prueba matcheó específicamente una de las 7 reglas con `token_requestor_id="Valid"` (categorías AFT/token muy angostas, y el motor es first-match-wins — plausible que jurisdicción Europa ya matcheara con una regla anterior antes de llegar a esas 7). Tampoco se pudo probar `cashback` con matching real — ninguna vigencia de esa regla (ni vieja ni nueva) cubre enero 2026 (la última vigencia cierra 2025-06-30). La validación de "no rompe nada" está confirmada; la validación de "el criterio filtra correctamente cuando debería decidir un match" queda solo a nivel de código + función aislada, no probada end-to-end con datos reales.

**Pendiente:** commitear los cambios (lo hace el usuario). Sin decidir todavía si se reprocesa EBGR/SBSA completos (enero 2026) con las reglas nuevas, ni si se replica en otros clientes.

---

## Por qué join_with_ardef() resuelve solapamientos de rangos ARDEF por transacción, no load_visa_ardef() de forma global (2026-07-27, EN CÓDIGO, SIN DESPLEGAR)

**Decisión:** `load_visa_ardef()` (`glue/scripts/visa/calculate/calculate.py`) ya no intenta dejar el ARDEF sin rangos solapados antes de tocar ninguna transacción — solo deduplica por `low_key_for_range` (`ORDER BY effective_date DESC, table_key DESC`). El DataFrame resultante puede seguir teniendo rangos que se solapan entre sí. `join_with_ardef()` resuelve cuál rango gana **por transacción**, después del join, con el mismo criterio (`effective_date DESC, table_key DESC`).

**Por qué:** investigando el bug de Hallazgo 5b (`product_id` incorrecto en cuentas donde un rango ARDEF viejo y ancho solapa con uno nuevo y angosto — ver `.claude/memory/gotchas.md`), se revisó el adapter real de legacy (`tst_files/python_scripts/adapters.py`, líneas ~2285-2306 y ~2382) para validar un primer intento de fix (self-join a nivel de ARDEF que eliminaba rangos dominados). Se encontró que **legacy nunca pre-resuelve el ARDEF a un conjunto sin solapamiento**: dedupea solo por `low_key_for_range` (CTE `ardef_pre_r`), hace `INNER JOIN` de las transacciones contra TODOS los rangos candidatos vía `BETWEEN`, y resuelve el ganador por transacción con `ROW_NUMBER() OVER (PARTITION BY app_id, app_hash_file ORDER BY app_date_valid DESC, high_key_for_range DESC)`.

El primer intento de fix (self-join eliminando rangos a nivel de ARDEF) resultó tener un problema real: al descartar rangos completos dominados por cualquier rango más nuevo que los solape, puede eliminar un rango que sigue siendo el ganador correcto para el subconjunto de cuentas que el rango más nuevo NO cubre (anidamiento a 3+ niveles con cobertura parcial distinta). Resolver por transacción, como legacy, es la única forma correcta para cualquier profundidad de anidamiento — porque se evalúa fresco contra la cuenta real de cada transacción en vez de eliminar candidatos de forma global.

**Alternativa descartada:** self-join a nivel de ARDEF que elimina rangos dominados por `effective_date` (2 iteraciones intentadas, ver `.claude/memory/gotchas.md`). Descartada por el problema de anidamiento a 3+ niveles arriba, y porque además reveló un bug preexistente en el dedup por `low_key_for_range` (sin desempate real — un empate perfecto en el `ORDER BY`, resultado no determinístico en Spark) que ya afectaba al pipeline antes de cualquier intento de fix.

**Costo aceptado:** `join_with_ardef()` ahora conserva `effective_date`/`table_key` un paso más (hasta después de su dedup post-join, que ya existía en el código original para el caso de múltiples matches dentro del mismo bucket de prefijo) — costo marginal, 2 columnas adicionales en un broadcast join que ya se hacía.

**Estado (actualizado 2026-07-28):** cambio de código aplicado, validado localmente en pandas y en Spark real. Subido a S3 y relanzado contra un archivo real de SBSA (`jr_f84e8113...`, SUCCEEDED) que cubre los 3 casos conocidos a la vez — CAL resultante confirma el flip correcto en los 3 (`402824050`→I, `402824060`→I, `415159016`→F). Reprocesado SBSA VI IN+OUT completo (enero 2026, 156 archivos, calculate→interchange→store) — 156/156 SUCCEEDED en las 3 etapas, 0 fallos. Comparativo final contra legacy (`glue-get-transaction`, `report_suffix=sbsa_202601_hallazgo5bfix`) confirma la mejora: `A/interregional` count -4→+0, `A/intraregional` fee -1,842.80→+28.41, `CEMEA GOLD` ya no aparece en la tabla de diferencias. Sin commitear — ver `.claude/memory/pending.md`.

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

**Decisión (actualizada 2026-06-11):** `lmbd-mc-store` (`_store_output`) ya NO usa `pd.concat(frames, axis=1)` por índice posicional. Ahora fusiona CLN + CAL + ITX con `df.merge(..., on=KEYS, how="left", validate="one_to_one")`, donde `KEYS = ["file_id", "file_idn", "ref_id"]`. Antes de cada merge: `_normalize_merge_keys()` castea las llaves a `string` nullable + `.str.strip()`; se valida que las 3 columnas existan en CLN/CAL/ITX y que no haya duplicados por `KEYS` (`raise ValueError` si fallan). Solo se agregan columnas de CAL/ITX que no existen ya en `merged`. CAL e ITX siguen opcionales: cualquier excepción de lectura/merge se loguea como warning y el merge continúa con lo disponible.

**Decisión anterior (superada):** `pd.concat(frames, axis=1)` — merge horizontal por índice posicional, asumiendo mismo número de filas y orden entre CLN/CAL/ITX.

**Razón del cambio:** El orden posicional entre etapas Spark (`glue-mc-calculate`/`glue-mc-interchange`) no está garantizado entre ejecuciones (shuffles/particionamiento) aunque el contenido sea el mismo. Un join por llave garantiza la fila correcta independientemente del orden físico. `ref_id` se agregó como tercera llave porque `file_id + file_idn` por sí solos no identifican de forma única cada mensaje IPM dentro de un archivo.

**Por qué falla rápido (`raise ValueError`):** un `how="left"` silencioso con llaves no únicas/faltantes produciría fan-out o nulls masivos en las columnas nuevas — mucho más difícil de detectar en Athena que en CloudWatch.

**Alternativa descartada:** Mantener el merge posicional + `orderBy` determinístico en CAL/ITX. Descartado por requerir el mismo orden en etapas independientes con sus propios shuffles/joins — más frágil que validar llaves explícitas.

**Impacto en `glue-mc-interchange`:** `run_interchange_mti()` ahora incluye `F.col("ref_id")` en la salida — requisito para que mc-store lo use como llave de merge.

Detalle completo → `.claude/memory/decisions_archive.md`.

---

## Por qué el router extrae la fecha MC descargando el archivo completo (decisión revertida, sync 2026-06-12)

**Decisión actual (sync 2026-06-12):** `extraer_fecha_mc()` descarga el archivo completo con un único `s3.get_object()`, y para archivos bloqueados aplica `_mc_unblock_full()` (replica `unblock_1014()` del interpreter, con `valid_seps` pushback) antes de escanear el trailer 695.

**Decisión anterior (2026-05-26, superada):** leía el archivo en chunks de 8 MB (overlap 8 KB) con `_mc_unblock_chunk` (siempre saltaba 2 bytes de separador sin verificar). Razón original: consistencia con Visa y evitar descargar archivos de hasta 1.5 GB solo por una fecha.

**Por qué se revirtió:** `_mc_unblock_chunk()` asumía que el separador de cada bloque de 1014 bytes eran siempre 2 bytes válidos. En archivos con separadores no estándar (≠`\x40\x40` EBCDIC space), esto desalineaba el stream desde el primer bloque "raro" — `_mc_scan_for_695()` nunca encontraba el trailer y caía al fallback `datetime.utcnow()`, registrando `file_date` incorrecto (afecta el particionamiento de todo el pipeline downstream).

`_mc_unblock_full()` corrige esto con `valid_seps = (b"\x40\x40", b"\x20\x20", b"\x00\x00", b"")`: si los 2 bytes tras cada bloque de 1012 no son un separador válido, hace `seek(-2)` (pushback) y los trata como parte del payload del siguiente bloque — misma lógica que `unblock_1014()` del interpreter. El pushback requiere conocer el byte siguiente al separador candidato, frágil de portar a través de límites de chunk — por eso se optó por descarga completa.

**Costo aceptado:** descarga completa adicional en el router para archivos MC bloqueados (cientos de MB–1.5 GB) — aceptado porque la alternativa (fecha incorrecta en `file_control`) es peor.

**Path de extracción sin cambios:** `DE48 del mensaje 695 → PDS tag "0105" → file_idn[3:9] → YYMMDD → YYYY-MM-DD`.

Detalle de implementación (`_mc_unblock_full`) → `.claude/memory/decisions_archive.md`.

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

**Razón:** Misma que Visa — sin esta columna no hay forma de saber qué archivo originó cada fila al consultar `operational` vía Athena, y el valor ya viaja en el payload del Step Function (`itl-0004-itx-dev-intchg-02-sfn-mc`) sin costo adicional.

**Diferencia de implementación con Visa:** En Visa, `lmbd-vi-clean` propaga `content_hash` automáticamente porque itera todas las columnas del input (que ya lo trae desde transform). En Mastercard, `glue-mc-calculate` (`process_file`) lo agrega explícitamente al final con `df_final = df_final.withColumn("content_hash", F.lit(content_hash))` (Spark `lit`, no viene como columna del CLN) — porque calculate es el primer punto del pipeline MC donde se trabaja en Spark y es más simple inyectarlo ahí que propagarlo columna a columna desde el interpreter (pandas) hasta calculate.

**Caso particular — `glue-mc-interchange` recibe `--content_hash` pero no lo usa:** El ASL de `itl-0004-itx-dev-intchg-02-sfn-mc` pasa `"--content_hash.$": "$.interchange_input.content_hash"` al job de interchange, pero `interchange.py` no declara ni usa ese argumento (Glue's `getResolvedOptions` simplemente lo ignora si no está en la lista de opciones requeridas). No es un bug — `content_hash` ya queda materializado en CAL (vía `glue-mc-calculate`) y mc-store lo hereda en el merge sin necesidad de que interchange lo reescriba. Si en el futuro `interchange.py` necesita `content_hash` para algo (logging, trazabilidad de su propio output), el argumento ya está disponible en el job sin cambios en el ASL.

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

## Por qué product_program_id en glue-vi-mc-reporting usa una nueva tabla `visa_bin_products` en s3-reference

**Decisión (2026-06-11):** `product_program_id` (antes `NULL` fijo, TODO documentado) se calcula ahora con un join: `product_id` (ya calculado en `glue-vi-calculate` via cruce ARDEF) → `bin_product_id` en `s3://itl-0004-itx-dev-intchg-02-s3-reference/visa_bin_products/data.parquet` (58 filas, export 1:1 de `m_visa_bin_products` legacy) → `range_program_id`.

**Implementación:** nueva función `load_visa_bin_products()`; `transform_visa_baseii()`/`transform_visa_sms()` reciben `vi_bin_products_df`, join `product_ref` (análogo a `merchant_country_ref`/`issuer_country_ref`).

**MC ya no es TODO (verificado 2026-07-01, esta nota estaba desactualizada):** `mastercard_bin_products/data.parquet` existe en s3-reference y `get_transaction.py` ya tiene `load_mastercard_bin_products()` (línea ~235) usado en `transform_mastercard()` con el mismo patrón (`gcms_product_identifier`/`licensed_product_identifier_pds_3` → `bin_product_id` → `range_program_id`). Confirmado también en el comparativo MC de EBGR/SBSA del 2026-06-30.

**Nota (2026-06-12):** refactor de nombres en `get_transaction.py` — variables legacy `m1/m2/m3/m5` renombradas a `merchant_country_ref`/`issuer_country_ref`/`product_ref`/`currency_alpha_ref`; sin cambios de lógica/resultados.

**Validación (2026-06-11):** re-run `glue-test-1` (`report_suffix=20260105_tst3`, EBGR 2026-01-01..2026-01-05) → SUCCEEDED. `product_program_id`: 0 nulls (antes 100%), suma=57,849,742=legacy, value_counts y mapeo `product_code→product_program_id` idénticos al legacy. **Resuelve completamente este TODO.**

**Alternativa descartada:** calcular en `glue-vi-calculate` (como los otros campos ARDEF). Descartado porque `product_program_id` es atributo del *producto* (tabla pequeña, 58 filas, cambia raramente) — más simple resolverlo via join liviano en el reporting job.

Detalle completo → `.claude/memory/decisions_archive.md`.

---

## Por qué glue-vi-mc-reporting (glue-test-1) lee `exchange_rate/rate_date=YYYY-MM-DD/` y no `exchange-rates/brand={brand}/exchange_date=YYYY-MM-DD/` (SUPERADA — ver decisión "Migración oficial a exchange-rates-glue" más abajo)

**Decisión (histórica, 2026-06-10, ya no vigente):** `load_exchange_rates()` en `glue/scripts/reports/get_transaction/get_transaction.py` lee `s3://itl-0004-itx-dev-intchg-02-s3-reference/exchange_rate/rate_date=YYYY-MM-DD/`, filtra por columna `brand` (`'VISA'` / `'MasterCard'`, comparación case-insensitive) y renombra columnas a `exchange_date, from_currency, to_currency, fx_rate`.

**Razón (histórica):** Existían dos ubicaciones de tipo de cambio en `s3-reference`:
- `exchange-rates/brand={Visa,MasterCard}/exchange_date=YYYY-MM-DD/` — cobertura incompleta en ese momento (no tenía todos los pares de moneda/fechas necesarios) y columnas sin códigos numéricos.
- `exchange_rate/rate_date=YYYY-MM-DD/` — cubre 2025-12-01..2026-04-30, ambas marcas en una sola tabla distinguidas por la columna `brand`. Tenía los pares de moneda necesarios en ese momento.

El bug original (`Column 'to_currency' does not exist`) solo se manifestó el 2026-06-10 porque hasta entonces el job fallaba ANTES (por el `SchemaColumnConvertNotSupportedException` de columnas NullType en `lmbd-vi-store`, ya resuelto) — nunca había llegado a ejecutar `_join_exchange_rates()`.

**Por qué quedó superada:** `exchange_rate/` era una fuente manual, congelada al 2026-04-30 (no crecía). Entretanto, `exchange-rates-glue/` (generada por el job `glue-exchange-rates`, que enriquece `exchange-rates/` con códigos numéricos del maestro `m_currency`) se convirtió en la fuente viva, oficial y con cobertura completa — ver decisión siguiente.

---

## Por qué se migró la fuente de tipo de cambio de `exchange_rate/` a `exchange-rates-glue/` en todo el pipeline (2026-07-08)

**Decisión:** Los 7 scripts que leían `exchange_rate/rate_date=YYYY-MM-DD/` (`glue-vi-interchange`, `glue-mc-interchange`, `glue-mc-calculate`, `glue-get-transaction`, `glue-scheme-fee`, `glue-vi-data-quality`, `glue-mc-data-quality`) fueron migrados a leer `exchange-rates-glue/brand={Visa,Mastercard}/exchange_date=YYYY-MM-DD/`. `exchange_rate/` ya no se lee en ningún script del pipeline.

**Razón:** `exchange_rate/` es una fuente manual, creada en algún momento como snapshot y nunca vuelta a alimentar — cobertura fija 2025-12-01..2026-04-30. `exchange-rates-glue/` es el producto oficial de un pipeline vivo: `lmbd-vi-exchange-rates`/`lmbd-mc-exchange-rates` scrapean tasas crudas (solo alfa) a `exchange-rates/`, y el job `glue-exchange-rates` (`format_exchange_rates.py`, antes `glue-test-2`) las enriquece con códigos numéricos del maestro `m_currency` y escribe a `exchange-rates-glue/` — cobertura 2025-06-01 en adelante, sigue creciendo. El usuario identificó esta relación entre las 3 fuentes y pidió oficializar `exchange-rates-glue/` como la única fuente del pipeline.

**Verificación de seguridad antes de migrar:** se comparó `exchange_rate/` vs `exchange-rates-glue/` para múltiples fechas de enero 2026 (ambas marcas, ~50K pares de moneda) — **0% de diferencia, valores bit-idénticos**. Esto redujo el riesgo de que el cambio moviera algún fee ya validado contra legacy.

**Validación tras el cambio:** `glue-vi-interchange`, `glue-mc-interchange` y `glue-mc-calculate` — cada uno re-ejecutado contra un archivo real ya procesado (EBGR, 2026-01-03) y comparado byte a byte contra el resultado anterior: **diff=0 exacto** en las 3. `glue-get-transaction` — no se pudo comparar directo contra un reporte baseline antiguo (`report_transactions_EBGR_202601_v2.parquet`) porque esa comparación dio un residual de +4,510 en `interchange_fees_amount` de VISA; investigado y confirmado que el residual era por el baseline desactualizado (operational reprocesado después de generarse ese reporte por razones no relacionadas), no por el cambio — se hizo una prueba A/B controlada (versión vieja vs nueva del script, mismo snapshot operational, back to back) que dio **diff=0 exacto** en `transaction_amount` e `interchange_fees_amount`, ambas marcas. `glue-scheme-fee` corrido con la fuente nueva sin error (EBGR, `--mode generate`, 202601). `glue-vi-data-quality` corrido como smoke test (nunca antes ejecutado con datos reales) — SUCCEEDED. `glue-mc-data-quality` no se validó (nunca se había ejecutado antes, sin baseline con el que comparar — detenido a pedido del usuario, sin impacto porque no está en producción).

**Alternativa descartada:** mantener `exchange_rate/` y solo ampliar su cobertura manualmente — descartado porque perpetúa un proceso manual cuando ya existe un pipeline automático (`glue-exchange-rates`) que resuelve lo mismo sin intervención.

---

## Por qué se agregaron business_transaction_cycle y settlement_report_currency_code en glue-vi-calculate

**Decisión (2026-06-11):** Dos campos nuevos en `glue/scripts/visa/calculate/calculate.py` (BASEII 28→30, SMS 26→27), requeridos por `get_transaction.py` y no materializados antes en `calculate.parquet`.

- **`business_transaction_cycle`** (BASEII/draft, `calc_business_transaction_cycle_draft`): deriva de `draft_code`+`usage_code` replicando la clasificación del SP legacy (purchase/reversal/chargeback × usage_code → código 1-255; cualquier otro `draft_code` → 255). SMS (`calc_business_transaction_cycle_sms`): siempre `NULL` (igual que legacy).
- **`settlement_report_currency_code`** (solo BASEII/draft, `calc_settlement_report_currency_code_draft`): `local_currency_code` del cliente (tabla `client-02`) si `calc_jurisdiction∈(on-us,off-us)` y `settlement_flag!=0`, sino `settlement_currency_code`. Sin equivalente SMS.

**Validación (2026-06-11):** contra `93BF199C85D2DF243AFDABEE5572E8C0` (EBGR, 2026-01-03, 269,725 filas BASEII): ambos campos 0 nulls, distribución/mapeo correctos (100% `"EUR"` para EBGR).

**Reproceso masivo:** los 100 archivos EBGR/VISA/IN de enero 2026 (calculate+store) reprocesados y confirmados en catálogo Glue tras re-crawl. Detalle en `manual_execution.md` → "Sesión 2026-06-11".

Detalle completo (tabla de mapeo draft_code/usage_code → business_transaction_cycle) → `.claude/memory/decisions_archive.md`.

---

## Por qué lmbd-mc-transform filtra list_parquet_files por prefijo file_id

**Decisión (sync 2026-06-11):** `list_parquet_files` en `lambdas/mastercard/transform/src/handler.py` ahora filtra los Parquets RAW del interpreter (`100_IPM_{MTI}_RAW/`) por `stem.upper().startswith(file_id.upper())` antes de procesarlos.

**Razón:** Mismo problema y misma solución ya documentados para `glue-mc-interchange` (ver gotcha "glue-mc-interchange: filtra por file_id para no reprocesar ejecuciones anteriores"). Sin el filtro, una re-ejecución de transform para un `file_id` listaría TODOS los Parquets RAW de la partición `file_type=X/date=YYYY-MM-DD` (incluyendo los de otros `file_id` ya procesados ese mismo día), reprocesándolos innecesariamente y potencialmente mezclando outputs de archivos distintos en el mismo `200_IPM_{MTI}_TRA/`.

**Alternativa descartada:** Ninguna — es la aplicación directa del patrón ya validado, no requirió evaluar alternativas.

---

## Por qué se agregó WaitForENIRelease (180s) entre Calculate e Interchange en itl-0004-itx-dev-intchg-02-sfn-mc

**Decisión (sync 2026-06-11):** En `step-functions/mastercard/asl.json`, el estado `CheckCalculateResult` (Choice) ahora tiene como `Default` un nuevo estado `WaitForENIRelease` (Type=Wait, Seconds=180), que a su vez transiciona a `PrepareInterchangeInput` (el destino anterior de `Default`).

**Razón:** `glue-mc-calculate` corre con conexión a VPC (para acceder a recursos de red privados). Cuando un job Glue con conexión VPC termina, AWS tarda en liberar las ENIs (Elastic Network Interfaces) asociadas — si `glue-mc-interchange` se lanza inmediatamente después, puede fallar o quedarse bloqueado esperando ENIs disponibles (límite de ENIs por subnet/cuenta). El Wait de 180s da margen para que AWS complete la liberación antes de lanzar el siguiente job.

**Alternativa descartada:** Reintentos con backoff en `glue-mc-interchange` ante fallos de ENI. Descartado por ser más complejo de diagnosticar (el error de ENI no siempre es claro) y porque un Wait fijo es más simple y predecible para este caso conocido.

**Cambio relacionado — MaxConcurrentRuns 20→50:** En el mismo sync, `glue-mc-calculate` y `glue-mc-interchange` pasaron de `MaxConcurrentRuns=20` a `50` (igual que `glue-vi-calculate`/`glue-vi-interchange`), habilitando el mismo patrón de reproceso masivo paralelo (`tst_files/reprocessing/reprocess_vi_*.py`) para Mastercard cuando se necesite.

---

## Por qué lmbd-mc-exchange-rates se reescribió con scraping vía proxies (ProxyManager + orquestador/worker encadenado)

**Decisión (sync 2026-06-11):** `lambdas/mastercard/exchange-rates/src/handler.py` reescrito (+678 líneas) para scrapear la API pública del conversor Mastercard (`mccom-services/currency-conversions/conversion-rates`), en vez de la fuente anterior.

**Componentes nuevos:** `ProxyManager` (pool round-robin, banea un proxy tras `PROXY_BAN_AFTER=1` fallo, enmascara credenciales en logs); `validate_proxies()` (descarta proxies muertos al arrancar); arquitectura orquestador/worker encadenada (`mode="orchestrator"|"worker"`) — el orquestador divide los pares de moneda en `NUM_CHUNKS=10` por fecha e invoca el primer worker; cada worker procesa su chunk con `ThreadPoolExecutor(MAX_WORKERS=9)`, escribe a `exchange_date={date}/` y se auto-relanza para el siguiente chunk — evita el timeout de 900s.

**Razón:** Mastercard no expone API oficial de tipos de cambio históricos; la API pública del conversor aplica rate-limiting/bloqueo agresivo por IP — requiere rotar proxies y espaciar requests (1.0-1.3s).

**Archivos nuevos:** `resources/currencies.json` (commiteado, catálogo de pares de moneda); `resources/proxy_settings.json` (**NO commiteado**, credenciales reales de proxy — en `.gitignore`; verificar que no quede tracked).

**Config asociada:** Timeout 300s→750s, MemorySize 2048→512MB (I/O-bound), VpcConfig removido (necesita internet público).

**Alternativa descartada:** mantener la fuente anterior — no cubría los pares de moneda/fechas necesarios para `glue-mc-calculate`/`glue-mc-interchange`.

Detalle completo (firma de cada componente, estructura de `proxy_settings.json`) → `.claude/memory/decisions_archive.md`.

---

## Por qué lmbd-mc-clean y lmbd-mc-extract leen/escriben Parquet en streaming (iter_batches + ParquetWriter) — sync 2026-06-12

**Decisión:** `_clean_*`/`_extract_*` dejaron de cargar el Parquet completo a un DataFrame + serializar todo de una vez. Ahora: `pq.ParquetFile` (solo footer) → `iter_batches(batch_size=ITX_CLEAN_BATCH_SIZE|ITX_EXTRACT_BATCH_SIZE, default=100000)` → por batch: `to_pandas()` → transformar (cast/rename/align) → `pa.Table` → `ParquetWriter.write_table()` a un `BytesIO` en memoria → `del`+`gc.collect()` por iteración → una sola subida final a S3. En mc-clean, el schema Arrow (`_build_arrow_schema`) se construye una vez (primer batch) y se reutiliza — extraído a `_align_df_to_schema()`.

**Razón:** los Parquets de entrada (RAW/TRA) pueden tener millones de filas — cargar el DataFrame completo + copias intermedias multiplica memoria varias veces sobre `MemorySize=10240MB` (ya al máximo). El streaming acota el pico a `batch_size` filas.

**Config asociada:** `mc-clean` Timeout 300s→600s; `mc-interpreter` EphemeralStorage 512MB→1536MB.

**`lmbd-mc-interpreter` — mismo principio en `_process_block`:** itera `block_buffer` en sub-chunks de `ITX_INTERPRETER_BLOCK_CHUNK_SIZE` (default 10000), construye `df_chunk` y escribe por sub-chunk vía `write_parquet_by_mti_block_streaming()`, liberando memoria (`del`+`gc.collect()`+`release_unused()`) entre sub-chunks.

**Nota:** `ITX_CLEAN_BATCH_SIZE`/`ITX_EXTRACT_BATCH_SIZE`/`ITX_INTERPRETER_BLOCK_CHUNK_SIZE` no declaradas en `config.json` (default seguro vía `os.environ.get`).

**Aplicable a mc-transform:** mismo patrón resolvería el gotcha pendiente "mc-transform: sin chunking en MTIs 1442, 1740 y 1644".

**Alternativa descartada:** aumentar `MemorySize` más allá de 10240MB — no es posible (máximo de AWS Lambda).

Detalle completo → `.claude/memory/decisions_archive.md`.

---

## Por qué lmbd-mc-store restaura el schema Arrow del CLN antes de escribir operational (mismo patrón que `_cal_dtype_map` en lmbd-vi-store)

**Decisión (2026-06-13):** `_store_output()` lee el CLN con `_read_parquet_s3_with_schema()` (`pq.read_table()`) y captura `cln_dtype_map = {nombre: tipo Arrow}` para todas las columnas excepto `KEYS`. Tras el merge CLN+CAL+ITX, convierte a `pa.Table` y llama `_restore_schema(merged_table, cln_dtype_map)`: columnas en `cln_dtype_map` → castea al tipo original (timestamps siempre `pa.timestamp("us")`); columnas nuevas (CAL/ITX) → `NullType→string`, `decimal128(p,s)→decimal128(18,s)` si `p≠18`, `timestamp→"us"`. Cast `safe=False`; si falla, warning y queda con tipo inferido (no aborta el archivo). Escribe con `_write_table_s3()` (schema explícito) — `_write_parquet_s3()` (sin schema) se eliminó.

**Razón:** mismo problema del gotcha "operational MC (IPM_1240/1442): TIMESTAMP(NANOS) y tipos inconsistentes...". El CLN ya tiene schema explícito consistente (`_build_arrow_schema`), pero `pd.read_parquet()`+`df.to_parquet()` sin schema re-infería tipos por archivo (`int64+nulls→double`, `string 100%-null→NullType`, `decimal128(18,s)→decimal128(p_inferido,s)`), rompiendo `spark.read.parquet()` con `SchemaColumnConvertNotSupportedException` y `AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`.

Mismo principio que deja a Visa en "0 inconsistencias" (`_cal_dtype_map`, ver decisión de lmbd-vi-store). **Diferencia con Visa:** en Visa el schema autoritativo es el CAL; en MC es el CLN (CAL/ITX de Spark/Glue no tienen schema fijo declarado).

**`function_code`:** confirmado columna del CLN (no CAL/ITX) — `cln_dtype_map` la restaura a `int64` sin cast adicional.

**Validación local (2026-06-13):** `tst_files/debug_scripts/test_mc_store_local.py` contra triple real (IPM_1240, EBGR, 2026-01-01, 22,419 filas, 148 columnas): 29 columnas corregidas, 0 remanentes con NullType/decimal≠18/timestamp≠us.

**Despliegue y reproceso (2026-06-13):** handler subido; `reprocess_mc_store.py` IN (120/120) + OUT (18/18) SUCCESS; `scan_mc_operational_schema_variance.py` → 0 inconsistencias en IPM_1240 (138 archivos) e IPM_1442 (5 archivos); `spark.read.parquet()` confirmado sin error (3,783,441 + 1 filas).

**Alternativa descartada:** `enableVectorizedReader=false` en `get_transaction.py` — habría mitigado el síntoma sin corregir el dato físico.

**Limpieza get_transaction.py (2026-06-13):** eliminado código Option A sin uso (`_NANOS_AS_LONG_COLS`, `_widest_arrow_type`, `_align_table_to_schema`, `_read_operational_via_pyarrow`, exclusión NullType, `spark.sql.legacy.parquet.nanosAsLong` + imports asociados). `read_operational()` quedó como `spark.read.parquet()` simple. Re-validado (`report_suffix=20260102_0105_mc_v3`).

Detalle completo (tabla de las 29 columnas corregidas) → `.claude/memory/decisions_archive.md`.

**Extensión 2026-07-09:** el mismo problema (int64+nulls→float64 por roundtrip pandas) también afectaba a columnas *nuevas* de CAL/ITX (ej. `jurisdiction_region`), sin captura de schema Arrow previa en `mc-store`. Fix: `_read_parquet_arrow_s3()` aplicado también a CAL/ITX. **Insuficiente por sí solo** — la causa raíz real estaba un paso antes, en `glue-mc-calculate` (`save_parquet()` escribía CAL sin schema explícito). Ver gotcha "jurisdiction_region/settlement_report_amount..." en `gotchas.md` para el fix completo y su validación.
