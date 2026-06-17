# Decisiones archivadas (detalle completo)

Este archivo contiene el detalle completo (implementación, validación, reproceso) de decisiones que fueron resumidas en `decisions.md` para reducir el contexto cargado automáticamente en cada conversación (este archivo **no** está referenciado vía `@` en CLAUDE.md).

Cada entrada aquí tiene su versión resumida correspondiente en `decisions.md` con un pointer hacia esta. Si necesitas el detalle completo de implementación/validación, búscalo aquí.

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

## Por qué el router extrae la fecha MC descargando el archivo completo (decisión revertida, sync 2026-06-12)

**Decisión anterior (2026-05-26, superada):** `extraer_fecha_mc()` leía el archivo IPM en chunks de 8 MB buscando el primer trailer MTI 1644 / FC 695, con overlap de 8 KB entre chunks y `_mc_unblock_chunk` (siempre salta 2 bytes de separador) para archivos bloqueados. Razón original: consistencia con Visa (solo primeros 50 bytes del header) y evitar descargar archivos de hasta 1.5 GB solo para una fecha.

**Decisión actual (sync 2026-06-12):** `extraer_fecha_mc()` descarga el archivo completo con un único `s3.get_object()`, y para archivos bloqueados aplica una nueva función `_mc_unblock_full()` (replica `unblock_1014()` del interpreter, con `valid_seps` pushback) antes de escanear el trailer 695.

**Por qué se revirtió:** `_mc_unblock_chunk()` siempre asume que el separador de cada bloque de 1014 bytes son 2 bytes válidos y los descarta sin verificar. En archivos con separadores no estándar (distintos de `\x40\x40` EBCDIC space), esto desalinea el stream desde el primer bloque con separador "raro" — todo lo que viene después queda corrido 0/1/2 bytes respecto a los límites reales de mensaje, y `_mc_scan_for_695()` nunca encuentra el trailer 695 (cae al fallback `datetime.utcnow()`, registrando una fecha incorrecta en `file_control`).

`_mc_unblock_full()` corrige esto con `valid_seps = (b"\x40\x40", b"\x20\x20", b"\x00\x00", b"")`: si los 2 bytes leídos después de cada bloque de 1012 no son un separador válido, hace `seek` hacia atrás y los trata como parte del payload del siguiente bloque (pushback) — exactamente la lógica de `unblock_1014()` en `lmbd-mc-interpreter`. Esta lógica de pushback requiere conocer el byte siguiente al separador candidato, lo que es frágil de implementar correctamente a través de límites de chunk (un separador podría quedar partido entre dos chunks) — por eso la solución pasó a descarga completa en vez de intentar portar el pushback al esquema de chunks+overlap.

**Costo aceptado:** los archivos MC bloqueados pueden ser grandes (cientos de MB–1.5 GB), por lo que esta función ahora hace una descarga completa adicional en el router (antes de que Step Functions/transform vuelvan a leer el mismo archivo). Se aceptó el costo porque la alternativa (fecha incorrecta en `file_control` para archivos con separadores no estándar) es peor — `file_date` se usa para particionar todo el pipeline downstream.

**Detalles de implementación vigentes:**
- `_mc_unblock_full(data, payload_size=1012, sep_size=2)`: lee el archivo completo en bloques de 1012 bytes; tras cada bloque lee 2 bytes y, si no están en `valid_seps`, hace `seek(-2)` (pushback) antes de leer el siguiente bloque.
- Archivos bloqueados (`file_block=True`): `data = _mc_unblock_full(raw)` antes de escanear
- Sin guardias de tamaño (`MAX_CHUNKS` eliminado) — se escanea el archivo completo en memoria
- Path de extracción sin cambios: `DE48 del mensaje 695 → PDS tag "0105" → file_idn[3:9] → YYMMDD → YYYY-MM-DD`

---

## Por qué product_program_id en glue-vi-mc-reporting usa una nueva tabla `visa_bin_products` en s3-reference

**Decisión (2026-06-11):** `product_program_id` (antes `NULL` fijo, TODO documentado) se calcula ahora con un join: `product_id` (ya calculado en `glue-vi-calculate` via cruce ARDEF, presente en CLN/operational) → `bin_product_id` en `s3://itl-0004-itx-dev-intchg-02-s3-reference/visa_bin_products/data.parquet` → `range_program_id`.

**Origen de la tabla:** export CSV de la tabla maestra `m_visa_bin_products` de PostgreSQL legacy (58 filas: `bin_product_id, short_description, bin_card_type, range_program_id, app_creation_date, app_creation_user`), provisto por el usuario y convertido 1:1 a Parquet (sin transformación) en `visa_bin_products/data.parquet`.

**Implementación en `get_transaction.py`:**
- `load_visa_bin_products()` — nueva función junto a `load_country()`/`load_exchange_rates()`, selecciona solo `bin_product_id, range_program_id`.
- `transform_visa_baseii()` y `transform_visa_sms()` reciben `vi_bin_products_df` como parámetro; join `product_ref` (análogo a `merchant_country_ref`/`issuer_country_ref` para country): `df["product_id"] == product_ref["_bin_product_id"]`, left join, alias `product_program_id_raw` → `product_program_id`.
- `process_client_range()` y `main()` propagan `vi_bin_products_df` (cargado y cacheado una vez, igual que `country_df`).
- MC (`product_program_id` en `transform_mastercard`) queda como TODO separado — requiere `m_mastercard_bin_products`, tabla distinta no provista aún.

**Nota (2026-06-12):** Refactor de legibilidad en `get_transaction.py` — las variables heredadas de la nomenclatura del SP legacy (`m1`, `m2`, `m3`, `m5`) se renombraron a nombres descriptivos: `merchant_country_ref`, `issuer_country_ref`, `product_ref`, `currency_alpha_ref` (MC). Mismo refactor reordenó la carga de tablas de referencia en `main()`/`process_client_range()` agrupándolas Visa-primero-luego-Mastercard, y limpió comentarios redundantes. Sin cambios de lógica/resultados — solo nombres y orden de parámetros.

**Validación (2026-06-11):** Re-run `glue-test-1` (`jr_f374a87d3849a2d8f4fa1c762c9ece25a4ba51b21118f4233460476253d73f65`, `report_suffix=20260105_tst3`, EBGR 2026-01-01..2026-01-05) → SUCCEEDED. `product_program_id`: 0 nulls (antes 100% null), suma=57,849,742=legacy, value_counts idénticos (103→555,220, 102→6,491), mapeo `product_code→product_program_id` idéntico (E,F,N,P→103; G→102) en ambos sistemas. **Resuelve completamente este TODO.**

**Alternativa descartada:** Calcular `product_program_id` directamente en `glue-vi-calculate` (como los otros campos ARDEF). Descartado porque `product_program_id` es un atributo del *producto* (tabla pequeña, 58 filas, cambia raramente) no de la transacción — más simple resolverlo en el reporting job via join liviano que mantenerlo sincronizado en cada `calculate.parquet`.

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

---

## Por qué lmbd-mc-clean y lmbd-mc-extract leen/escriben Parquet en streaming (iter_batches + ParquetWriter) — sync 2026-06-12

**Decisión:** `_clean_1644`/`_clean_standard` (mc-clean) y `_extract_1644`/`_extract_standard` (mc-extract) dejaron de hacer `df = _read_parquet(key)` (carga el Parquet completo a un solo DataFrame) seguido de `_write_parquet_with_schema(df, ...)` (serializa todo de una vez). Ahora:

1. Descargan los bytes del Parquet a un `io.BytesIO` y abren un `pq.ParquetFile` (solo lee el footer).
2. Iteran `pf.iter_batches(batch_size=ITX_CLEAN_BATCH_SIZE | ITX_EXTRACT_BATCH_SIZE, default=100000)`.
3. Por cada batch: `to_pandas()` → transformación (cast/rename/align) → `pa.Table` → `writer.write_table()` sobre un `pq.ParquetWriter` que escribe a otro `io.BytesIO` en memoria.
4. `del` + `gc.collect()` en cada iteración para liberar el batch anterior antes de procesar el siguiente.
5. Al final, `out_buf.seek(0)` + `S3.put_object(..., Body=out_buf)` — una sola subida del Parquet completo, pero nunca un DataFrame completo en memoria.

En mc-clean, el schema Arrow (`_build_arrow_schema`) se construye una sola vez a partir del primer batch del primer archivo y se reutiliza para todos los archivos del mismo MTI — se extrajo a una función nueva `_align_df_to_schema()` (devuelve `pa.Table`, compartida por `_write_parquet_with_schema` y el loop de batches).

**Razón:** Los Parquets de entrada de mc-clean/mc-extract (capas RAW/TRA del interpreter/transform) pueden tener millones de filas — cargar el DataFrame completo + las copias intermedias de cada transformación (`_cast_df`, `_align_df_1644`, rename, etc.) multiplica el uso de memoria varias veces sobre el tamaño del Parquet. Con `MemorySize=10240MB` ya al máximo, esto causaba presión de memoria en archivos grandes. El streaming por batches acota el pico de memoria a `batch_size` filas independientemente del tamaño total del archivo.

**Cambios de configuración asociados:** `mc-clean` `Timeout` 300s→600s (compensar el overhead de iterar en batches vs. una sola pasada — más iteraciones de Python, no más trabajo total). `mc-interpreter` `EphemeralStorage` 512MB→1536MB (relacionado, ver `_process_block` abajo).

**`lmbd-mc-interpreter` — mismo principio aplicado a `_process_block`:** antes construía `wide_rows` (lista de dicts) para TODO `block_buffer` de una vez (un bloque puede tener 400K+ mensajes ISO-8583 con 80+ columnas cada uno) y luego un solo `pd.DataFrame(wide_rows)`. Ahora itera `block_buffer` en sub-chunks de `ITX_INTERPRETER_BLOCK_CHUNK_SIZE` (default 10000) mensajes, construye `wide_rows`/`df_chunk` por sub-chunk, llama a `write_parquet_by_mti_block_streaming()` por sub-chunk, y libera (`del` + `gc.collect()` + `pa.default_memory_pool().release_unused()`) antes del siguiente. `file_idn`/`file_dt` (del trailer 695, ya conocidos antes de llamar a `_process_block`) se aplican igual a cada sub-chunk — no hay riesgo de procesar un sub-chunk sin contexto.

**Nota — env vars sin declarar en config.json:** `ITX_CLEAN_BATCH_SIZE`, `ITX_EXTRACT_BATCH_SIZE` e `ITX_INTERPRETER_BLOCK_CHUNK_SIZE` se leen con `os.environ.get(..., "100000"/"10000")` pero ninguna está declarada en `config.json`/`env-vars.json` — mismo patrón (con default seguro, por lo que no es bug latente como `DDB_MASTERCARD_FIELDS_TABLE`) pero conviene agregarlas si se necesita tunearlas por ambiente.

**Aplicable a mc-transform:** este es el mismo patrón que resolvería el gotcha pendiente "mc-transform: sin chunking en MTIs 1442, 1740 y 1644" — replicar `iter_batches` + `ParquetWriter` ahí cuando se aborde ese pendiente.

**Alternativa descartada:** Aumentar `MemorySize` más allá de 10240MB. No es posible — 10240MB es el máximo de AWS Lambda.

---

## Por qué lmbd-mc-store restaura el schema Arrow del CLN antes de escribir operational (mismo patrón que `_cal_dtype_map` en lmbd-vi-store)

**Decisión (2026-06-13):** `_store_output()` en `lambdas/mastercard/store/src/handler.py` ahora:
1. Lee el CLN con `_read_parquet_s3_with_schema()` (usa `pq.read_table()` en vez de `pd.read_parquet()`) y captura `cln_dtype_map = {nombre: tipo Arrow}` para todas las columnas excepto `KEYS` (`file_id`, `file_idn`, `ref_id` — `_normalize_merge_keys` ya las castea a string nullable para el merge, independientemente de su tipo original).
2. Tras el merge CLN+CAL+ITX, convierte `merged` a `pa.Table.from_pandas(merged, preserve_index=False)`.
3. Llama a `_restore_schema(merged_table, cln_dtype_map)`, que para cada columna:
   - Si está en `cln_dtype_map`: castea al tipo original del CLN — **excepto timestamps, siempre forzados a `pa.timestamp("us")`**.
   - Si no está en `cln_dtype_map` (columnas CAL/ITX, ej. `settlement_report_amount`, `function_code`): `NullType→string`, `decimal128(p,s)→decimal128(18,s)` si `p != 18`, `timestamp→"us"`.
   - Cada cast usa `safe=False`; si falla (`ArrowInvalid`/`ArrowNotImplementedError`), se loguea warning y la columna queda con su tipo inferido (no aborta el archivo).
4. Escribe con `_write_table_s3()` (`pq.write_table`, schema explícito) en vez de `_write_parquet_s3()` (`df.to_parquet()` sin schema) — esta última función se eliminó (quedó sin uso).

**Razón:** Mismo problema documentado en el gotcha "operational MC (IPM_1240/1442): TIMESTAMP(NANOS) y tipos inconsistentes entre archivos rompen spark.read.parquet()". El CLN de `lmbd-mc-clean` ya tiene un schema explícito y consistente (`_build_arrow_schema`, desde DynamoDB `mastercard_fields`: `decimal128(18,scale)`, `int64`/`int32`, `timestamp("ns")`, `string`), pero `pd.read_parquet()` + `df.to_parquet()` sin schema en mc-store hacía que pyarrow re-infiriera tipos **por archivo** a partir de los valores pandas: `int64+nulls→double`, `string 100%-null→NullType(INT32)`, `decimal128(18,s)→decimal128(p_inferido,s)`. Esto rompía `spark.read.parquet()` sobre el directorio completo con `SchemaColumnConvertNotSupportedException` (tipos físicos distintos entre archivos) y `AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))` (el lector vectorizado de Spark ignora `spark.sql.legacy.parquet.nanosAsLong=true`).

Es exactamente el mismo principio que ya deja a Visa en "0 inconsistencias" (`_cal_dtype_map` en `lmbd-vi-store`, ver decisión "Por qué lmbd-vi-store lee el CAL con _read_parquet_arrow..."), validado empíricamente: 268 columnas / 57 archivos `EBGR/VISA/baseii_drafts/file_type=IN/`, 0 inconsistencias. Detalle completo del análisis comparativo VI vs MC en `tst_files/findings/mc_vs_vi_operational_type_consistency.md`.

**Diferencia con el caso Visa:** En Visa el schema autoritativo se toma del **CAL** (`lmbd-vi-store` solo restaura columnas del CAL porque CLN ya era consistente). En Mastercard el schema autoritativo se toma del **CLN** (`_build_arrow_schema` en `lmbd-mc-clean`), porque CAL/ITX (Spark/Glue) no tienen un schema fijo declarado de la misma forma — sus columnas nuevas (ej. `settlement_report_amount`) se normalizan con reglas genéricas (NullType→string, decimal→precision 18) en vez de restaurarse a un tipo "original".

**`function_code` resuelto (sin pendiente separado):** la prueba local (ver abajo) mostró que `function_code` es columna del **CLN**, no CAL/ITX como se había asumido en el análisis inicial — `cln_dtype_map` la restaura a `int64` igual que las demás columnas categoría 1 (`double→int64`). No requiere `.cast()` adicional en `glue-mc-calculate`/`glue-mc-interchange`.

**Validación local (2026-06-13):** `tst_files/debug_scripts/test_mc_store_local.py` replicó el merge CLN+CAL+ITX + `_restore_schema` contra un triple real (IPM_1240, EBGR, 2026-01-01, 22,419 filas, 148 columnas) descargado a `tst_files/mc_store_test/`. Resultado: 29 columnas corregidas (9× `double→int64` incl. `function_code`, 8× `NullType→string`, 9× `decimal128(p,*)→decimal128(18,*)`, 1× `timestamp[ns]→timestamp[us]`, + `date_action_de_73: null→date32[day]` y `settlement_report_amount: decimal128(8,4)→decimal128(18,4)`), **0 columnas remanentes** con NullType/decimal≠18/timestamp≠us, escritura/relectura Parquet OK.

**Despliegue y reproceso completados (2026-06-13):** (1) usuario subió `handler.py` al Lambda `lmbd-mc-store`; (2) `tst_files/reprocessing/reprocess_mc_store.py` para `file_type=IN` (120 archivos `EBGR/MC/IN` DONE en `file_control-02`) → 120/120 SUCCESS, y luego `reprocess_mc_store.py OUT` para los 18 archivos `file_type=OUT` que sí tenían `store_result` (de 82 DONE, 64 no lo tienen — ver hallazgo separado para el equipo MC en `tst_files/findings/mc_vs_vi_operational_type_consistency.md`) → 18/18 SUCCESS; (3) `scan_mc_operational_schema_variance.py` → **0 inconsistencias** en `IPM_1240` (138 archivos) e `IPM_1442` (5 archivos); (4) `spark.read.parquet()` lee `EBGR/MC/IPM_1240/` (3,783,441 filas) e `IPM_1442/` (1 fila) sin error — confirmado con `glue-test-1` (`report_suffix=20260102_0105_mc_v2/v3`).

**Alternativa descartada:** `enableVectorizedReader=false` en `get_transaction.py` (propuesta inicial, no probada, finalmente no necesaria) — habría solo mitigado el síntoma en el job de reporting sin corregir el dato físico en `operational`; cualquier otro consumidor (Athena, otros Glue jobs) habría seguido viendo tipos inconsistentes entre archivos. El fix real fue corregir el dato en origen (`lmbd-mc-store`).

**Limpieza de get_transaction.py (2026-06-13):** una vez confirmado que `spark.read.parquet()` lee operational MC sin fallback, se eliminó de `glue/scripts/reports/get_transaction/get_transaction.py` todo el código del Option A que ya no se usaba: `_NANOS_AS_LONG_COLS`, `_widest_arrow_type`, `_align_table_to_schema`, `_read_operational_via_pyarrow`, la exclusión de columnas `NullType` y la config `spark.sql.legacy.parquet.nanosAsLong` (más los imports de `pyarrow`/`pyarrow.parquet`/`pyarrow.fs`/`urlparse`/`StructType`/`NullType`/`from_arrow_schema`, ya sin uso). `read_operational()` quedó como un `spark.read.parquet()` simple con `try/except`. Re-validado con `report_suffix=20260102_0105_mc_v3` → mismos conteos, reporte de 4,092,878 filas escrito en `s3-analytics`. Detalle en `mc_operational_nanos_reader_issue.md`.
