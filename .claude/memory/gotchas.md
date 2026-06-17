# Gotchas y problemas conocidos

Problemas encontrados durante el desarrollo, con su causa raíz y solución recomendada. Verificar si siguen vigentes antes de actuar. Gotchas resueltos y validados con detalle completo (síntoma, debugging, validación paso a paso) fueron movidos a `.claude/memory/gotchas_archive.md` (no cargado automáticamente) — cada entrada resumida abajo tiene su pointer correspondiente.

---

## glue-vi-calculate: calc_vss_aggregation_level — lógica recursiva daba nivel 2 a todas las hojas — RESUELTO (pendiente validar)

**Archivo:** `glue/scripts/visa/calculate/calculate.py` (función `calc_vss_aggregation_level`)
**Detectado/corregido:** sync 2026-06-12 (cambio traído de AWS, no documentado hasta ahora)

**Causa raíz:** la implementación anterior navegaba la jerarquía de rollup hacia arriba en 3 iteraciones (`for level in [1, 2, 3]`) para distinguir niveles intermedios. Como el `rollup_to` de cualquier nodo hoja **siempre** pertenece al `rollup_group` (conjunto de todos los `rollup_to != reporting_for`) por definición, la primera iteración (`level=1`) ya marcaba a todas las hojas como nivel 1, y por la forma del loop terminaban en nivel 2 — nunca se asignaba el nivel 0 esperado para hojas.

**Fix aplicado:** reescritura completa a una clasificación directa sin recursión:
- `10` (raíz): `rollup_to == reporting_for`
- `1` (nodo intermedio/padre): `rollup_to != reporting_for` **y** `reporting_for` ∈ `rollup_group` (este nodo es a su vez destino de rollup de otra fila)
- `0` (hoja): `rollup_to != reporting_for` y `reporting_for` ∉ `rollup_group`

Implementado con un único `LEFT JOIN` contra `rollup_group_df` (distinct de `rollup_to` donde `rollup_to != reporting_for`, broadcast) + `F.when(...)` anidado, sin columnas temporales de iteración (`_row_id`, `_current_reporting`, etc. eliminadas).

**Impacto:** afecta `calc_vss_aggregation_level` para VSS_110/120/130/140 — cualquier reporte o validación que dependa de `vss_aggregation_level == 0` (hojas) para filtrar registros de detalle estaba recibiendo `2` en su lugar.

**Si vuelve a aparecer (`vss_aggregation_level` no tiene valores `0`, o tiene `2` donde debería haber `0`):** verificar que el script en S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py`) tenga la versión join-based, no la recursiva de 3 iteraciones.

**Estado:** código corregido (ya está en S3/AWS, traído al repo via sync 2026-06-13). Pendiente: (1) re-ejecutar `glue-vi-calculate` + `lmbd-vi-store` para algún `file_id` con registros VSS y validar `value_counts(vss_aggregation_level)` — debe incluir `0` para hojas; (2) revisar si algún reporte (`get_transaction.py`) o el contraste DQ contra VSS en `glue-vi-interchange` asumía el valor incorrecto `2`.

---

## lmbd-router: extraer_fecha_mc() con _mc_unblock_chunk desalineaba archivos bloqueados con separadores no estándar — RESUELTO

**Archivo:** `lambdas/router/src/handler.py` (función `extraer_fecha_mc`)
**Corregido:** sync 2026-06-12 (cambio traído de AWS, no documentado hasta ahora)

`_mc_unblock_chunk()` descartaba siempre 2 bytes por cada bloque de 1014, sin verificar que fueran un separador válido (`\x40\x40`). En archivos bloqueados con separadores no estándar, esto desalineaba el stream desde el primer bloque "raro" y `_mc_scan_for_695()` nunca encontraba el trailer 695 → `file_date` caía al fallback `datetime.utcnow()` (fecha incorrecta en `file_control`, particiones erróneas en todo el pipeline downstream).

**Fix:** `extraer_fecha_mc()` ahora descarga el archivo completo y usa `_mc_unblock_full()` (replica `unblock_1014()` del interpreter con `valid_seps` pushback). Detalle completo de la decisión y por qué no se pudo mantener el esquema de chunks → `decisions.md` → "Por qué el router extrae la fecha MC descargando el archivo completo".

**Estado:** Resuelto (en AWS desde 2026-06-12). Si vuelve a aparecer `file_date` = fecha de hoy para un archivo MC bloqueado, sospechar de esto primero.

---

## glue-test-1 (glue-vi-mc-reporting): load_exchange_rates() leía tabla incompleta y con columnas incorrectas — RESUELTO Y VALIDADO

**Archivo:** `glue/scripts/reports/get_transaction/get_transaction.py` (función `load_exchange_rates`)

`load_exchange_rates()` leía `exchange-rates/brand={brand}/exchange_date=.../` con columnas inexistentes (`from_currency`/`to_currency`/`fx_rate`) y cobertura incompleta. Fix: lee `exchange_rate/rate_date=YYYY-MM-DD/` (cubre 2025-12-01..2026-04-30, ambas marcas via columna `brand`) y renombra columnas. Validado 2026-06-11: reporte EBGR generado en s3-analytics (561,711 filas, 32 columnas).

**Pendientes que sigue exponiendo:** `scheme_fees_amount` (TODO, flujo no implementado), validación SMS/MC (skeletons `# VERIFY`), escaneo NullType en `SBSA`/`BTRLRO`/`vss_110-140` antes de generar reportes para esos clientes/tipos. `product_program_id` ya resuelto (ver memoria de usuario `visa_bin_products_join.md`).

**Estado:** Resuelto y validado. Detalle completo (síntoma, causa raíz, debugging, validación paso a paso) → `.claude/memory/gotchas_archive.md`.

---

## glue-test-1 (glue-vi-mc-reporting): operational MC (IPM_1240/1442) — TIMESTAMP(NANOS) y tipos inconsistentes entre archivos rompen spark.read.parquet(); fallback PyArrow causa OOM — PENDIENTE

**Archivo:** `glue/scripts/reports/get_transaction/get_transaction.py` (función `read_operational`, helpers `_read_operational_via_pyarrow`/`_widest_arrow_type`/`_align_table_to_schema`, SparkSession config)
**Detectado:** 2026-06-13

**Síntoma:** `spark.read.parquet("s3://.../EBGR/MC/IPM_1240/")` (138 archivos) falla en la etapa de scan (no en inferencia de schema) con `AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))` pese a tener `spark.sql.legacy.parquet.nanosAsLong=true` configurado. El `except` de `read_operational()` detecta el mensaje y entra al fallback `_read_operational_via_pyarrow` (lee todos los archivos con PyArrow, unifica schema, concatena, `to_pandas()`, `spark.createDataFrame()`) — pero ~2 min después el job termina con `User application exited with 137` (OOM kill): cargar 138 archivos completos en memoria del driver no escala.

**Causa raíz:** `spark.sql.legacy.parquet.nanosAsLong=true` solo aplica al lector Parquet **no vectorizado**. El lector **vectorizado** (default, `spark.sql.parquet.enableVectorizedReader=true`) ignora el flag y lanza la misma excepción para columnas `TIMESTAMP(NANOS)` (`date_and_time_local_transaction_de_12`). El lector vectorizado es también el que lanza `SchemaColumnConvertNotSupportedException` cuando una columna tiene tipo físico distinto entre archivos (decimal con distinta precisión, int32 vs decimal, etc. — ~25 columnas en IPM_1240/1442). El lector no vectorizado convierte ambos casos genéricamente via `Cast`.

**Fix propuesto, NO probado:** agregar `.config("spark.sql.parquet.enableVectorizedReader", "false")` a la SparkSession (junto a `nanosAsLong`). Si funciona, `spark.read.parquet()` leería IPM_1240/1442 directo sin pasar por el fallback PyArrow (eliminando el OOM), y dejaría `_read_operational_via_pyarrow`/`_widest_arrow_type`/`_align_table_to_schema` como código removible tras validar.

**Alternativa de corto plazo (discutida, no aplicada):** comentar temporalmente la lectura/transform MC en `get_transaction.py` para generar un reporte VI-only y desbloquear la comparación VI vs CSV legacy mientras se valida el fix de Spark conf.

**Estado:** Pendiente — VI funciona (309,436 filas EBGR `baseii_drafts`). Detalle completo (logs, ids de run, próximos pasos) en memoria de usuario `mc_operational_nanos_reader_issue.md`.

---

## lmbd-vi-store: columnas NullType en operational rompen lectura de directorio completo con Spark (SchemaColumnConvertNotSupportedException) — RESUELTO

**Archivo:** `lambdas/visa/store/src/handler.py` (función `store_output`)

Columnas del CAL 100% null para ciertos `file_id` (`message_reason_code`, `type_of_purchase`) se degradaban a NullType (INT32) en el round-trip pandas/pyarrow, rompiendo `spark.read.parquet(directorio)` cuando convivían con archivos donde la columna sí tenía `string`. Fix: generalización de `_cal_int_cols` → `_cal_dtype_map` (restaura NullType→string además de float64→int64). Reprocesados 56/56 archivos `EBGR/VISA/baseii_drafts/file_type=IN` (2026-01-01..2026-01-30) — 0 columnas NullType tras el fix; validado por el re-run de `glue-test-1` (gotcha anterior).

**Caso adicional (2026-06-16) — 3 archivos con `status=PARTIAL_SUCCESS`:** Detectado al comparar el reporte EBGR enero 2026 completo (report_suffix=202601_v2) contra legacy: las fechas 2026-01-20, 2026-01-21 y 2026-01-29 mostraban ~13% de las filas esperadas. Causa: esos 3 archivos habían sido procesados originalmente **antes** de que el fix de `_cal_dtype_map` estuviera desplegado — `output_type=BASEII` falló por NullType durante el procesamiento original y DynamoDB quedó con `status=PARTIAL_SUCCESS` (solo VSS_110/120/130/140 se habían escrito). El `output_type=BASEII` nunca fue escrito en operational. Reprocesados con `lmbd-vi-store` (solo BASEII, CLN/CAL/ITX confirmados presentes en S3): 3/3 SUCCESS. Tras crawler re-run y re-ejecución del reporte: VI count = 4,051,482 / 4,051,482 (diff=0). **Señal de alerta:** si el comparativo con legacy muestra que ciertas fechas tienen ~13% de las filas esperadas (no 0%), verificar `status` en DynamoDB `file_control-02` — puede ser un `PARTIAL_SUCCESS` silencioso, no un error de datos.

**Cómo identificar `PARTIAL_SUCCESS` en DynamoDB:**
```powershell
aws dynamodb get-item `
  --profile itx-dev `
  --table-name itl-0004-itx-dev-dynamo-file_control-02 `
  --key '{"file_id": {"S": "<file_id>"}}' `
  --query "Item.{status:control_status.S, store_result:store_result.S}"
```
Si `store_result.outputs[]` no incluye `output_type=BASEII`, ese output falló.

**Distinción `file_id` vs `content_hash`:** DynamoDB `file_control-02` usa `file_id` como PK (no `content_hash`). Si se tiene solo el `content_hash`, usar `scan` con `filter-expression "content_hash = :h"` para obtener el `file_id` real. En el caso de Jan 20: `file_id=0A8221C3293EF535621FB1E35D709ACC` (PK) pero `content_hash=F308708F2709F2F83AF7C692B33BA292` (distinto).

**Pendiente:** verificar el mismo problema en `SBSA`/`BTRLRO` y otros `output_type` (VSS_110/120/130/140) si sus reportes fallan con la misma excepción.

**Si vuelve a aparecer:** usar `tst_files/debug_scripts/scan_nulltype_columns.py` para listar archivos/columnas afectadas, mapear via `file_control` (scan por rango de fechas + `control_status=PARTIAL_SUCCESS`) y reprocesar con `lmbd-vi-store`.

**Estado:** Resuelto y validado para EBGR enero 2026 completo (2026-06-16). Detalle completo (debugging, escaneo, reprocesamiento) → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: fillna(0.0) en fee_min/fee_cap zeroeaba fees positivos — RESUELTO (pendiente validar re-run)

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `process_pandas_partitions`)
**Detectado:** 2026-06-09

**Causa raíz:** `.fillna(0.0)` sobre `interchange_fee_cap`/`interchange_fee_min` convertía `NaN` (reglas sin cap/min) en `0.0`. Spark recibe `0.0` como valor real (no NULL) → `coalesce(0.0, ±inf) = 0.0` → `least(fee_amount, 0.0) = 0`, zeroeando todos los fees positivos de esas reglas (y flooreando los negativos por el lado de `fee_min`). Detectado al comparar `sum(interchange_fee_amount)` por jurisdiction/source_currency vs legacy (off-us EUR: −289 USD).

**Fix aplicado:** Eliminado `.fillna(0.0)` — dejar solo `.astype(float)`. NaN→NULL en Spark→`coalesce(NULL,±inf)`→sin restricción.

**Si vuelve a aparecer:** verificar que no haya `fillna(0.0)` sobre esas dos columnas antes del yield — solo `.astype(float)`.

**Estado:** código corregido y subido a S3 (2026-06-09). Pendiente re-ejecutar `glue-vi-interchange` + `lmbd-vi-store` y re-validar la comparación por jurisdiction/source_currency (ver también las entradas siguientes de _apply_default y content_hash — mismas pendientes de re-run, se pueden validar juntas en una sola corrida). Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: matching incorrecto intelica_id ATM JPY — regla 1055 en vez de 1065 — PENDIENTE

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (motor de reglas `_apply_default` / `_evaluate_rules_pandas`)
**Detectado:** 2026-06-09

**Síntoma:** En la comparación de `sum(interchange_fee_amount)` por jurisdiction/source_currency, la diferencia residual de −29.64 para interregional JPY (source_currency=392) se debe a que el nuevo sistema asigna `intelica_id=1055` ("ATM AF") mientras el legacy asigna `intelica_id=1065` ("ATM AF JPN").

**Detalle de las reglas vigentes al 2026-01-03:**

| intelica_id | fee_descriptor | fee_variable | fee_fixed | fee_currency |
|---|---|---|---|---|
| 1055 | ATM AF | 0.0015 | — | None (source_ccy) |
| 1065 | ATM AF JPN | 0.0015 | 0.50 | USD |

Simulación para source_amount=20,220 JPY:
- Legacy (1065): `0.0015 × 20,220 × exchange(JPY→USD) + 0.50 = 0.19 + 0.50 = 0.69 USD`
- Nuevo (1055): `0.0015 × 20,220 = 30.33 JPY`

Nota: los fee_amounts están en **monedas distintas** (USD vs JPY) — no son comparables como número directo.

**Causa probable:** La regla 1065 "ATM AF JPN" tiene alguna condición que la restringe a transacciones de Japón (issuer_country, acquirer_country, o merchant_country). Esa condición existe en `visa_rules` pero el motor de reglas del nuevo sistema no la está evaluando correctamente o no está presente en el `calculate.parquet` para esa transacción.

**Para investigar:** Comparar los campos de condición entre la regla 1065 y la 1055 en `visa_rules.parquet` (ambas vigentes al 2026-01-03) para identificar qué campo diferencia "ATM AF JPN" de "ATM AF". Verificar que ese campo tenga el valor correcto en `calculate.parquet` para la transacción en cuestión.

**Estado:** Pendiente de investigación. La diferencia de −29.64 en la comparación global es 1 transacción (count=1).

---

## glue-vi-interchange: dirección del exchange_value — pendiente validar convención

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `calculate_fee_amounts`)
**Detectado:** 2026-06-09

**Contexto:** Existen dos fórmulas posibles para `interchange_fee_amount`, con resultados distintos en transacciones cross-currency:

| Sistema | Fórmula | Moneda del resultado |
|---|---|---|
| Legacy PostgreSQL | `fee_variable × (source_amount × exchange_value) + fee_fixed` | fee_currency |
| Prototipo local | `fee_variable × source_amount + fee_fixed × exchange_value` | source_currency (si exchange_value = source_ccy/fee_ccy) |
| Glue actual | `fee_variable × source_amount + fee_fixed × exchange_value` | depende de convención |

El usuario prefiere que el fee se exprese en **source_currency** ("la regla se adapta a la moneda de la transacción"). La fórmula del prototipo es consistente con eso SI `exchange_value` en la tabla S3 almacena `source_ccy/fee_ccy` (convención inversa a la del legacy).

**Para validar:** Leer `s3://itl-0004-itx-dev-intchg-02-s3-reference/exchange_rate/data.parquet`, filtrar `currency_from=EUR, currency_to=USD`, ver si `exchange_value ≈ 1.08` (fee_ccy/source_ccy, convención legacy) o `≈ 0.926` (source_ccy/fee_ccy, convención prototipo).

**Estado:** Pendiente — validar convención del exchange_value antes de decidir si la fórmula actual de `calculate_fee_amounts` es correcta.

---

## glue-vi-calculate: calc_timeliness_draft fórmula de domingos tenía off-by-one — no cuadraba con legacy — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py` (función `calc_timeliness_draft`)
**Detectado:** 2026-06-09

**Causa raíz:** La fórmula original (`full_weeks + extra_sunday` con `extra_sunday = when(remaining >= days_to_next_sunday, 1)`) contaba un domingo de más cuando `remaining == days_to_next_sunday` (ese domingo cae justo fuera de la ventana `[purchase+1, central-1]`).

**Fix aplicado:** Reescritura a fórmula directa con offset: `offset = (8 - start_dow) % 7`; `sundays = max(0, floor((total_days - 1 + 6 - offset) / 7))`.

**Si vuelve a aparecer:** la discrepancia (timeliness 1 de menos que legacy) aparece solo cuando `(total_days - 1) % 7 == (8 - start_dow) % 7` — cualquier lógica `remaining >= days_to_next_sunday` tiene este off-by-one.

**Estado:** Resuelto en código local (2026-06-09), incluido en el `calculate.py` usado en el reproceso masivo EBGR enero 2026 (sesión 2026-06-11), pero no se validó específicamente `timeliness` contra legacy tras ese reproceso. Detalle completo (derivación, ejemplo numérico) → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: _apply_default() convertía NaN a cadena "nan" en columnas no-SPACE — filas excluidas de reglas válidas — RESUELTO

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `_apply_default`)
**Detectado:** 2026-06-09

**Causa raíz:** `batch[col].astype(str).str.strip()` convierte `NaN → "nan"` (len=3), por lo que `.mask(len==0, "BLANK")` no lo sustituye — la columna queda con la cadena `"nan"`, que no matchea contra `valid_values` (e.g. `['Y','N','BLANK']`) y la fila cae al fallback.

**Fix aplicado:** `batch[col].fillna("").astype(str).str.strip()` antes del `.mask(...)` — garantiza `NaN → "" → "BLANK"`.

**Si vuelve a aparecer:** verificar que toda normalización de condiciones use `fillna("").astype(str).str.strip()`, nunca `astype(str)` directo sobre columnas con nulls.

**Estado:** Resuelto en código local (2026-06-09). Pendiente subir a S3 y re-ejecutar `glue-vi-interchange` (ver nota de re-run consolidada en la entrada de fillna(0.0) arriba). Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: content_hash se perdía en el Parquet ITX por mapInPandas — RESUELTO (pendiente validar tras re-run)

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `evaluate_interchange_fees`)
**Detectado:** 2026-06-08

**Causa raíz:** `evaluate_interchange_fees()` usa `mapInPandas()`, que reemplaza el schema completo — `content_hash` llegaba como columna de entrada (propagado desde clean/calculate) pero no estaba declarado en `OUTPUT_COLS`/`output_schema`, así que se descartaba silenciosamente. Job terminaba SUCCESS, conteo correcto, sin la columna.

**Fix aplicado:** agregado `"content_hash"` como primer elemento de `OUTPUT_COLS` y `StructField("content_hash", StringType(), True)` como primer campo de `output_schema`.

**Si vuelve a aparecer (columna ausente pese a estar en la lista de columnas finales):** sospechar de un `mapInPandas`/`applyInPandas` intermedio que reemplaza el schema — la columna debe declararse tanto en la salida del iterador como en el `StructType`.

**Estado:** código corregido y subido a S3 (2026-06-08). Pendiente re-ejecutar `glue-vi-interchange` y validar que `content_hash` es la primera columna del `itx.parquet` (ver nota de re-run consolidada en la entrada de fillna(0.0) arriba). Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: _apply_default() destruía el token "Space" (espacio literal) — transacciones GR caían en regla fallback — RESUELTO (pendiente validar tras re-run)

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `_apply_default`)
**Detectado:** 2026-06-08

**Causa raíz:** un `value = value.strip()` extra (no presente en el prototipo local validado) convertía el espacio literal `' '` en `''` al parsear criterios tipo `"Space,9"` → `valid_values=['','9']`. Transacciones GR con `acceptance_terminal_indicator=' '` (que está en `COLUMN_GROUP_SPACE`, sin normalizar) no matcheaban `intelica_id=39` ("GR SECURE CR") y caían en el fallback `63` ("GR NON-SEC CR"). Validado contra producción: 524 transacciones GR cumplían TODAS las demás condiciones de la regla 39.

**Fix aplicado:** eliminado el `.strip()` extra dentro del loop de `value_list`.

**Si vuelve a aparecer:** verificar que ningún `.strip()`/normalización adicional se aplique a `value_list` después de `replace("SPACE", " ")` — el espacio literal debe sobrevivir hasta el `isin()`.

**Estado:** código corregido y subido a S3 (2026-06-08). Pendiente re-ejecutar `glue-vi-interchange` y confirmar que esas 524 transacciones obtienen `intelica_id=39` (ver nota de re-run consolidada en la entrada de fillna(0.0) arriba). Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-calculate: load_visa_ardef() vaciaba el ARDEF por to_date() sin formato — campos ARDEF quedaban 100% null — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py` (función `load_visa_ardef`)

`F.to_date(F.col("effective_date"))` sin formato (campo real es `yyyyMMdd`) devolvía `NULL` para el 100% de las filas, vaciando el ARDEF y dejando los 10 campos derivados (`ardef_country`, `product_id`, etc.) 100% null. Fix: `F.to_date(F.col("effective_date"), "yyyyMMdd")` + eliminación de un pre-filtro de strings con formatos incompatibles.

**Estado:** Resuelto y validado (re-run 2026-06-06, match ~100% vs ARDEF local; reprocesado masivamente para EBGR enero 2026 en 2026-06-11). Detalle completo (metodología de comparación valor-a-valor) → `.claude/memory/gotchas_archive.md`.

---

## mc-interpreter: mensaje IPM con DE_55 corrupto desincronizaba el stream y abortaba el archivo completo — RESUELTO

**Archivo:** `lambdas/mastercard/interpreter/src/handler.py` (función `read_len_prefixed_messages_variable`)
**Detectado:** 2026-06-10

**Causa raíz:** Un `DE_55` con longitud declarada (120) distinta de la real (118 bytes) — anomalía del archivo fuente — desincronizaba la lectura de DEs subsecuentes. El handler anterior no tenía manejo de error: `KeyError`/`ValueError` no controlados abortaban el generador completo, perdiendo TODOS los bloques ya procesados (porque `finalize_writers`/`upload_tmp_outputs` solo corren si el generador termina sin excepción).

**Fix aplicado:** se portó el mecanismo de resync del legacy (`_resync_stream` + `_valid_mti_byte_patterns`, parametrizado por encoding cp500/latin-1): ante cualquier fallo de parseo de DE (`parameters.get(i)` sin KeyError, `parse_ok=False; break` en todos los casos de error), se busca el siguiente mensaje válido escaneando `record_length` + MTI plausibles; si no se encuentra, se hace `break` preservando los bloques ya procesados (a diferencia del `on_error=True` del legacy que descartaba todo el archivo).

**Validación (2026-06-10):** lectura completa del archivo de prueba (422,734 mensajes), 100% `parse_ok=True`, 2 mensajes corruptos consecutivos descartados via resync.

**Si vuelve a aparecer (lectura se detiene antes del final / `KeyError`/`ValueError` no controlado):** revisar log `WARNING ... Mensaje corrupto descartado ... RESYNC exitoso/fallido`; si el resync falla repetidamente cerca del mismo offset, sospechar corrupción real del archivo fuente.

**Estado:** Resuelto en código local (2026-06-10). Pendiente subir el handler al Lambda `lmbd-mc-interpreter` y validar end-to-end con `itl-0004-itx-dev-intchg-02-sfn-mc`. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## mc-transform: timeout con múltiples MTIs (riesgo alto)

**Archivo:** `lambdas/mastercard/transform/src/handler.py`
**Detectado:** 2026-05-22

**Problema:** El handler procesa los 4 MTIs (1240, 1442, 1644, 1740) secuencialmente en una sola invocación. Si todos están presentes en el archivo, puede superar fácilmente el timeout de 400s.

**Solución recomendada:** Que Step Functions invoque el Lambda una vez por MTI, pasando el MTI como parámetro — igual que el patrón ya usado en el flujo Visa.

**Estado:** Pendiente de resolver antes de validación end-to-end.

---

## mc-transform: sin chunking en MTIs 1442, 1740 y 1644 (riesgo medio)

**Archivo:** `lambdas/mastercard/transform/src/handler.py`
**Detectado:** 2026-05-22

**Problema:** Solo `transform_ipm_1240` implementa chunking dinámico. Los MTIs 1442, 1740 y 1644 cargan el Parquet completo en memoria, lo que puede causar OOM en archivos grandes.

**Solución recomendada:** Replicar el patrón de chunking de `transform_ipm_1240` en los otros tres MTIs.

**Estado:** Pendiente.

---

## mc-transform: EphemeralStorage /tmp insuficiente (riesgo medio)

**Archivo:** `lambdas/mastercard/transform/src/handler.py`  
**Config:** `lambdas/mastercard/transform/config.json`
**Detectado:** 2026-05-22

**Problema:** `transform_ipm_1240` escribe un Parquet completo en `/tmp` antes de subirlo a S3. El EphemeralStorage por defecto es 512 MB, insuficiente para archivos Mastercard grandes.

**Solución recomendada:** Aumentar EphemeralStorage a 2048 MB+ en la config del Lambda, o cambiar la escritura para hacer stream directo a S3 (sin pasar por `/tmp`).

**Estado:** Pendiente.

---

## mc-transform: variable de entorno DDB_MASTERCARD_FIELDS_TABLE no declarada en config.json (bug latente)

**Archivo:** `lambdas/mastercard/transform/config.json`
**Detectado:** 2026-05-22

**Problema:** El código usa `DDB_MASTERCARD_FIELDS_TABLE` para consultar la tabla de campos Mastercard en DynamoDB, pero esta variable no está declarada en `config.json` ni en `env-vars.json`. Cae al valor hardcodeado `"itl-0004-itx-dev-dynamo-mastercard_fields-02"`, lo que romperá en ambientes distintos a dev.

**Solución recomendada:** Agregar `DDB_MASTERCARD_FIELDS_TABLE` a `config.json` y `env-vars.json` igual que las otras variables de entorno del Lambda.

**Confirmado aún pendiente (sync 2026-06-11):** El sync de Lambdas de esta sesión trajo cambios de código a `lmbd-mc-transform` (nuevo `content_hash` propagado + filtro `list_parquet_files` por `file_id`, ver `decisions.md`), pero el diff de `config.json` solo modificó `CodeSize`, `LastModified`, `CodeSha256` y `RevisionId` — el bloque `Environment.Variables` quedó sin cambios y `DDB_MASTERCARD_FIELDS_TABLE` sigue ausente. El bug latente sigue vigente.

**Estado:** Pendiente — bug latente que se manifestará al desplegar en ambiente empresarial.

---

## itx-extract comparte el rol IAM del router (deuda técnica)

**Detectado:** 2026-04-08 (CHANGELOG v1.0.0)

**Problema:** `lmbd-vi-extract` no tiene un rol IAM propio — comparte `itx-lambda-router-role`. Esto viola el principio de mínimo privilegio.

**Solución recomendada:** Crear `itx-lambda-extract-role` con solo los permisos que extract necesita (S3 read/write staging, DynamoDB read visa-fields).

**Estado:** Pendiente (documentado en CHANGELOG como tarea para el nuevo ambiente).

---

## glue-vi-calculate: Py4JError causado por toPandas() en load_visa_ardef — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py`

`load_visa_ardef` usaba `.toPandas()` + dedup/eliminación de solapamientos en pandas, presionando la heap del driver (OOM con archivos grandes) → `Py4JError` en la siguiente llamada a `logger.info()`. Fix (2026-06-02): migración completa a Spark (`Window.partitionBy` + `row_number()`/`F.lag()`), el ARDEF nunca sale de los executors.

**Estado:** Resuelto. Si reaparece `Py4JError` en este job, buscar `Java heap space`/`ExecutorLostFailure` en CloudWatch justo antes. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-mc-interchange: filtra por file_id para no reprocesar ejecuciones anteriores

**Archivo:** `glue/scripts/mastercard/interchange/interchange.py`

Sin filtro por `file_id`, el job reprocesaba TODOS los Parquets de la partición `file_type=X/date=YYYY-MM-DD`, incluyendo los de ejecuciones anteriores del mismo día. Fix: filtrar archivos listados por `stem_from_uri(path).upper().startswith(file_id.upper())` (aplica a TXN/CLN y CAL). Mismo patrón aplicado en `lmbd-mc-transform` (ver `decisions.md`).

**Estado:** Resuelto, por diseño desde la implementación inicial. Verificar el mismo patrón en `glue-vi-interchange` si aparece el mismo síntoma. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-calculate: timeliness debe ser LongType (bigint), NO IntegerType — HIVE_PARTITION_SCHEMA_MISMATCH — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py`

`.cast(IntegerType())` en `calc_timeliness_draft`/`calc_timeliness_sms` escribía `int` (INT32) en Parquets nuevos mientras particiones viejas tenían `bigint` (INT64) → Athena `HIVE_PARTITION_SCHEMA_MISMATCH`. Fix: `.cast(LongType())` en ambas funciones — todos los archivos quedan `bigint`.

**Estado:** Resuelto. Si reaparece, verificar que el script en S3 use `LongType()` y, si hay particiones mixtas, forzar `bigint` en el catálogo antes de re-crawl. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-calculate: tipos explícitos en funciones de cálculo numérico — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py`

Sin `.cast()` explícito, Spark infiere tipos que el crawler de Glue detecta incorrectamente (`double` en vez de `int`). Casts aplicados (2026-06-05): `calc_business_transaction_type_draft`/`calc_reversal_indicator_draft`/`calc_reversal_indicator_sms` → `IntegerType()`; `calc_surcharge_amount` → `DoubleType()`; `calc_timeliness_draft`/`calc_timeliness_sms` → `LongType()` (ver gotcha anterior).

**Regla:** toda nueva función de cálculo numérico debe terminar con `.cast(TipoExplícito)`.

**Estado:** Resuelto. Detalle completo (tabla) → `.claude/memory/gotchas_archive.md`.

---

## lmbd-vi-store: columnas enteras del CAL se escriben como double en operational — RESUELTO

**Archivo:** `lambdas/visa/store/src/handler.py`

`pq.read_table(...).to_pandas()` convierte INT64+nulls → `float64` (numpy no tiene int nullable); al reconstruir con `pa.Table.from_pandas()`, PyArrow infiere `double`, y el crawler detectaba `timeliness` (y otras columnas enteras del CAL) como `double` en vez de `bigint`/`int`. Fix (2026-06-05): leer CAL con `_read_parquet_arrow()`, capturar `_cal_int_cols` (tipos enteros del schema Arrow) y restaurar `float64 null → int64 null` con `.cast()` tras cada `pa.Table.from_pandas(merged)`.

**Estado:** Resuelto — generalizado después a `_cal_dtype_map` (ver gotcha de NullType en lmbd-vi-store arriba, también cubre NullType→string). Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-mc-interchange: solo procesa MTIs 1240 y 1442 (1644 y 1740 excluidos)

**Archivo:** `glue/scripts/mastercard/interchange/interchange.py`
**Detectado:** 2026-06-02

**Comportamiento:** El job llama a `run_interchange_mti()` únicamente para MTIs 1240 y 1442. Los MTIs 1644 (liquidación) y 1740 (fee collection) no tienen capa ITX generada por este job.

**Impacto en mc-store:** `MTIS_WITH_ITX = frozenset({"1240", "1442"})` — el store no intentará buscar `600_IPM_1644_ITX` ni `600_IPM_1740_ITX`, lo que es correcto.

**Estado:** Por diseño. No es un bug. Ver decisión en `decisions.md` sobre por qué no se contrasta contra 1644.

---

## lmbd-vi-clean: _parse_dates() lógica incorrecta para campos de fecha YDDD y MMDD — RESUELTO

**Archivo:** `lambdas/visa/clean/src/handler.py` (función `_parse_dates`)

La estrategia "compute-then-correct" (restar años si el resultado supera `file_date`) era incorrecta para los 3 formatos: `!YDDD` (`central_processing_date`/`account_reference_number_date`) restaba 10 años de más; `!MMDD` (`purchase_date`) comparaba fecha completa en vez de solo el mes; `conversion_date` necesitaba un nuevo formato `!YDDD_MAX` (cap a 1 año atrás si supera `file_date`). Reescritura completa de `_parse_dates()` con las 3 estrategias correctas + DynamoDB actualizado (`conversion_date.date_format = !YDDD_MAX`).

**Esquema real de `visa_fields-02`:** HASH=`type_record`, RANGE=`column_name` (CLAUDE.md decía `field_id` — corregido).

**Estado:** Resuelto y validado al 100% contra PostgreSQL legacy (0 nulls, 8/8 combinaciones campo/fecha coinciden), desplegado 2026-06-08. Detalle completo (tabla de validación, señales de regresión) → `.claude/memory/gotchas_archive.md`.

---

## Athena HIVE_BAD_DATA: columnas ARDEF (ardef_country, etc.) BINARY en Parquet vs integer en partición — RESUELTO

**Tabla:** `itl_0004_itx_dev_02_glue_database_operational_ebgr_visa.baseii_drafts`, partición `file_type=IN/date=2026-01-15`

Antes del fix de ARDEF (gotcha de `load_visa_ardef()` arriba), los 10 campos derivados eran 100% null y el crawler los tipó como `int` en la metadata de esa partición. Tras el fix, el Parquet físico tiene esas columnas como `BINARY` (string), pero la partición seguía con `int` en el catálogo → `HIVE_BAD_DATA`. Fix: re-correr el crawler (`UPDATE_IN_DATABASE`).

**Si vuelve a aparecer (`HIVE_BAD_DATA ... BINARY ... incompatible with int/double/etc.`):** comparar `aws glue get-partition` vs `get-table` para la columna/partición afectada y re-correr el crawler correspondiente. Causa típica: columna que fue 100% null por un bug ya corregido.

**Estado:** Resuelto y verificado en `operational_ebgr_visa` y `staging_ebgr_visa` (2026-06-10). Detalle completo → `.claude/memory/gotchas_archive.md`.
