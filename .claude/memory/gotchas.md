# Gotchas y problemas conocidos

Problemas encontrados durante el desarrollo, con su causa raíz y solución recomendada. Verificar si siguen vigentes antes de actuar.

---

## glue-test-1 (glue-vi-mc-reporting): load_exchange_rates() leía tabla incompleta y con columnas incorrectas — "Column 'to_currency' does not exist" — RESUELTO Y VALIDADO

**Archivo:** `glue/scripts/reports/get_transaction/get_transaction.py` (función `load_exchange_rates`, usada por `_join_exchange_rates`)
**Detectado:** 2026-06-10

**Síntoma:** Tras resolver el bug de columnas NullType (ver gotcha siguiente), se relanzó `glue-test-1` (JobRunId `jr_b0e8b19c35c6128524a4bb5cd8f137096938c453fe60c9a003630bc22c5b732c`) para EBGR 2026-01-01..2026-01-05. El job terminó `SUCCEEDED` (`Error=None`, `ExecutionTime=95s`) pero el log `/aws-glue/jobs/error` mostraba:
```
GlueLogger: [read_operational] Loaded EBGR/VISA/baseii_drafts: 561711 rows
GlueLogger: [BASEII] EBGR/2026-01-01_2026-01-05: Column 'to_currency' does not exist.
  Did you mean one of the following? [currency_to, currency_from, currency_to_code, exchange_date, currency_from_code, exchange_value];
...
[process_client_range] No data for EBGR/2026-01-01_2026-01-05
No data for EBGR in [2026-01-01, 2026-01-05], skipping
```
Es decir: los 561,711 registros de `baseii_drafts` se cargaron correctamente (confirmando que el fix de NullType funcionó), pero `_join_exchange_rates()` lanzó una excepción capturada silenciosamente que dejó la rama BASEII sin filas. Como EBGR no tiene SMS, el job terminó sin generar ningún reporte — `SUCCEEDED` con cero output.

**Causa raíz:** `load_exchange_rates()` leía `s3://{BUCKET_REF}/exchange-rates/brand={brand_path}/exchange_date=YYYY-MM-DD/` y asumía columnas `from_currency, to_currency, fx_rate, exchange_date` (declaradas en el docstring y en el schema del fallback vacío). El Parquet real en esa ruta tiene columnas `currency_from, currency_to, currency_from_code, currency_to_code, exchange_value` (+ `exchange_date` como partición Hive) — `_join_exchange_rates()` hace `xr.filter(F.col("to_currency") == report_currency)` y falla porque `to_currency` no existe.

Además, esa ruta (`exchange-rates/brand=Visa/`) tiene **cobertura incompleta**: no contiene el par `EUR→USD` para `exchange_date=2026-01-01` (necesario para el reporte EBGR, que reporta en EUR).

**Cómo se detectó:** `aws glue get-job-run ... --query "JobRun.{State,Error,ExecutionTime}"` mostraba `SUCCEEDED`, pero el usuario notó "un error raro en el log". Se descargó `/aws-glue/jobs/error` (stream=JobRunId, `MSYS_NO_PATHCONV=1 aws logs get-log-events ...`) y se hizo `grep -niE "error|exception"` — 2 líneas `ERROR` de `GlueLogger`, una de ellas el `Column 'to_currency' does not exist` con el plan Spark mostrando el schema real del DataFrame.

**Solución aplicada (2026-06-10):** `load_exchange_rates()` reescrita para leer `s3://{BUCKET_REF}/exchange_rate/rate_date=YYYY-MM-DD/` (cubre 2025-12-01..2026-04-30, ambas marcas en una tabla con columna `brand`='VISA'/'MasterCard'). Filtra `F.upper(F.col("brand")) == brand_path.upper()` y renombra `rate_date→exchange_date`, `currency_from→from_currency`, `currency_to→to_currency`, `exchange_value→fx_rate` — sin tocar `_join_exchange_rates()` ni las funciones `transform_*`. Validado: `VISA EUR→USD` existe en `rate_date=2026-01-05` (28,056 filas VISA, 22,650 MasterCard en ese día). Detalle de la decisión en `decisions.md` → "Por qué glue-vi-mc-reporting (glue-test-1) lee exchange_rate/rate_date=...".

Subido a `s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/get_transaction.py`. Re-lanzado: JobRunId `jr_ecbf44e09aa4db4cabceb597478ffc21b18b27a9b4dc02f7f020fe039c284c3d` (`report_suffix=20260105_tst2`).

**Validación (2026-06-11):** `jr_ecbf...284c3d` → `SUCCEEDED` (`Error=None`, `ExecutionTime=103s`). Output generado: `s3://itl-0004-itx-dev-intchg-02-s3-analytics/EBGR/reports/report_transactions_EBGR_20260105_tst2.parquet/` (16 MB, 561,711 filas, 32 columnas = `FINAL_COLS` completo). Verificado:
- `reported_currency_code`: 100% `"EUR"`, 0 nulls
- `interchange_fees_amount`: 0 nulls, valores no-cero (rule matching + conversión de moneda funcionando)
- `transaction_amount`: rango -2795..23898 (valores convertidos a EUR vía `xr1_rate`, no solo el monto crudo)
- `interchange_rule`: poblado con descriptores reales (`GR GP OCT CR`, `VE NON-SEC CR`, etc.)
- `product_program_id`: 100% null — esperado, sigue siendo TODO (join ARDEF BIN products, no implementado aún)
- `scheme_fees_amount`: 100% `0.0` — esperado, flujo aún no implementado (TODO documentado)

**Si vuelve a aparecer (`Column 'X' does not exist` en `_join_exchange_rates` o columnas de `load_exchange_rates`):** verificar el schema real de `s3://itl-0004-itx-dev-intchg-02-s3-reference/exchange_rate/rate_date=<fecha>/*.parquet` (columnas: `brand, currency_from, currency_to, currency_from_code, currency_to_code, exchange_value, year, month`) — puede haber cambiado si el nuevo método de extracción de tipo de cambio Visa (en desarrollo) reemplaza esta tabla.

**Nota:** hay un nuevo método de extracción de tipo de cambio Visa en desarrollo (mencionado por el usuario 2026-06-10) — cuando esté disponible, revisar si `load_exchange_rates()` debe apuntar a esa nueva fuente.

**Pendientes restantes para `get_transaction.py`:** `product_program_id` (join ARDEF BIN products, ver `reporting_job_design.md`), `scheme_fees_amount` (flujo TODO), validación SMS/MC (skeletons con `# VERIFY`), escaneo NullType en `SBSA`/`BTRLRO`/`vss_110-140` antes de generar reportes para esos clientes/tipos.

---

## lmbd-vi-store: columnas NullType en operational rompen lectura de directorio completo con Spark (SchemaColumnConvertNotSupportedException) — RESUELTO

**Archivo:** `lambdas/visa/store/src/handler.py` (función `store_output`)
**Detectado:** 2026-06-10

**Síntoma:** El reporting job `glue-test-1` (`get_transaction.py`) fallaba con:
```
SchemaColumnConvertNotSupportedException: column 'message_reason_code' ... Expected: string, Found: INT32
```
y luego, tras un primer fix parcial, con el mismo error en otra columna (`type_of_purchase`). Ocurría al hacer `spark.read.parquet(base_path)` sobre `EBGR/VISA/baseii_drafts/file_type=IN/date=2026-01-0X/` — un directorio con varios archivos Parquet (uno por `file_id`).

**Causa raíz:** Algunas columnas del CAL (`message_reason_code`, `type_of_purchase`, posiblemente otras) son **100% null** para ciertos `file_id`. En `lmbd-vi-store`, `pq.read_table(...).to_pandas()` representa esa columna como `object` con puros `None`; al reconstruir con `pa.Table.from_pandas(merged)`, PyArrow no puede inferir el tipo real y le asigna `pa.null()` (NullType → se escribe como `INT32` en Parquet). Otros archivos del mismo directorio, donde la columna SÍ tiene valores, la escriben correctamente como `string` (BINARY). `spark.read.parquet(directorio)` sin `mergeSchema` toma el schema de UN archivo como canónico para todo el directorio → el vectorized reader no soporta convertir `INT32 (NullType) ↔ BINARY (string)` → excepción.

**Cómo se detectó la magnitud real:** Se escribió `tst_files/scan_nulltype_columns.py` (lee solo el footer/schema de cada Parquet via `pyarrow.fs.S3FileSystem`, sin descargar el archivo completo) y se escaneraron los 56 archivos de `EBGR/VISA/baseii_drafts/file_type=IN/` (2026-01-01 a 2026-01-30). Resultado: **54 de 56** tenían `type_of_purchase` en NullType, y **27 de esos 54** además tenían `message_reason_code` en NullType. Solo estaba "limpio" lo ya reprocesado manualmente con el handler corregido.

**Solución aplicada (2026-06-10):** Generalización de `_cal_int_cols` → `_cal_dtype_map` en `lmbd-vi-store` (ver `decisions.md` → "Por qué lmbd-vi-store lee el CAL con _read_parquet_arrow..."). Ahora restaura tanto `int64+nulls→float64` como `string-100%-null→NullType` después de cada `pa.Table.from_pandas(merged)`. Desplegado por el usuario.

**Reprocesamiento masivo (2026-06-10):** Se reprocesaron los 56 archivos de `EBGR/VISA/baseii_drafts/file_type=IN` (2026-01-01..2026-01-30) invocando `lmbd-vi-store` (output_type=BASEII) con el handler corregido — 56/56 SUCCESS. Re-escaneo con `scan_nulltype_columns.py` confirmó **0 columnas NullType** en los 56 archivos. Tras esto, `glue-test-1` para el rango 2026-01-01..2026-01-05 se relanzó (JobRunId `jr_b0e8b19c35c6128524a4bb5cd8f137096938c453fe60c9a003630bc22c5b732c`).

**Si vuelve a aparecer (`SchemaColumnConvertNotSupportedException` leyendo cualquier directorio operational/staging con Spark):**
1. Identificar la columna y el archivo reportados en el error.
2. Correr `tst_files/scan_nulltype_columns.py` (ajustar `BUCKET`/`PREFIX`/cliente) para listar TODOS los archivos del directorio con columnas NullType — no asumir que es solo 1 columna/archivo, suele haber varias.
3. Mapear cada `content_hash` afectado a `file_id` via `file_control` (scan DynamoDB) y reprocesar con `lmbd-vi-store` (output_type correspondiente).
4. Verificar que el fix de `_cal_dtype_map` siga desplegado en el Lambda — si reaparece en archivos NUEVOS (no solo viejos), el fix se revirtió o no cubre la columna/caso nuevo.

**Pendiente:** Verificar el mismo problema en otros clientes (`SBSA`, `BTRLRO`) y otros `output_type` (VSS_110/120/130/140) si los reportes correspondientes fallan con el mismo tipo de excepción.

**Estado:** Resuelto para `EBGR/VISA/baseii_drafts/file_type=IN` (56/56 archivos). Pendiente confirmar resultado de `glue-test-1` (rango 2026-01-01..2026-01-05) tras el reprocesamiento.

---

## glue-vi-interchange: fillna(0.0) en fee_min/fee_cap zeroeaba fees positivos — RESUELTO

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `process_pandas_partitions`)
**Detectado:** 2026-06-09

**Síntoma:** `interchange_fee_amount` era 0 para transacciones que matcheaban reglas sin cap explícito (`fee_cap=NaN`). En la comparación por jurisdiction/source_currency, jurisdicciones off-us EUR mostraban fee_amount ≈289 USD menos que el legacy.

**Causa raíz:**

```python
# ANTES (bug):
result_pdf["interchange_fee_cap"] = result_pdf["interchange_fee_cap"].fillna(0.0).astype(float)
result_pdf["interchange_fee_min"] = result_pdf["interchange_fee_min"].fillna(0.0).astype(float)
```

Reglas sin cap definen `fee_cap=NaN` en pandas. `fillna(0.0)` lo convierte a `0.0`. Spark recibe `0.0` (no NULL), por lo que `coalesce` no actúa:
```python
F.coalesce(F.col("interchange_fee_cap"), F.lit(float("inf")))  # → coalesce(0.0, +inf) = 0.0
F.least(F.col("interchange_fee_amount"), 0.0)                  # → min(15.50, 0.0) = 0.0
```
Todos los fees positivos de esas reglas quedaban en cero. El mismo problema con `fee_min=NaN → 0.0` flooreaba fees negativos innecesariamente.

**Solución aplicada (2026-06-09):**
```python
# DESPUÉS (correcto):
result_pdf["interchange_fee_cap"] = result_pdf["interchange_fee_cap"].astype(float)
result_pdf["interchange_fee_min"] = result_pdf["interchange_fee_min"].astype(float)
```
NaN de pandas → NULL en Spark → `coalesce(NULL, ±inf)` → sin restricción. Para reglas con cap explícito (ej. `fee_cap=0.04`), el valor se conserva y actúa como cap real.

**Si vuelve a aparecer (fees en 0 para reglas que deberían tener fee positivo):** Verificar que no haya `fillna(0.0)` sobre `interchange_fee_cap` o `interchange_fee_min` antes del yield en `process_pandas_partitions`. El patrón correcto: solo `astype(float)`, sin fillna.

**Estado:** Resuelto en código local y subido a S3 (2026-06-09). Pendiente validar re-run.

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

**Síntoma:** Los valores de `timeliness` no coincidían con el legacy PostgreSQL para transacciones donde el residuo de la ventana (`remaining_days`) era exactamente igual a `days_to_next_sunday`. En esos casos el legacy daba N, el Glue job daba N−1 (1 domingo de más contado).

**Causa raíz:** La fórmula original construía la ventana `[purchase+1, central−1]` y contaba domingos con `full_weeks + extra_sunday`:

```python
_days_between = datediff(end_for_sundays, start_for_sundays) + 1
_full_weeks   = floor(days_between / 7)
_remaining    = days_between % 7
_dts          = when(start_dow == 1, 0).otherwise(8 - start_dow)  # días al próximo domingo
_extra_sunday = when(days_between > 0 AND remaining >= dts, 1).otherwise(0)
```

El bug: cuando `remaining == dts`, el próximo domingo cae en la posición `full_weeks*7 + dts = days_between` — exactamente fuera del intervalo (índices válidos: `0..days_between−1`). La condición `>=` lo contaba igual.

**Ejemplo concreto:**
- purchase=2026-01-04 (Dom), central=2026-01-18 (Dom), total_days=14
- Ventana [05-ene (Lun), 17-ene (Sáb)] — un único domingo: 11-ene
- Old: full_weeks=1, remaining=6, dts=6 → `6 >= 6` → extra=1 → sundays=2 ✗
- New: `max(0, floor((14 − 1 + 6 − 6) / 7)) = floor(13/7) = 1` ✓

**Solución aplicada (2026-06-09):** Reescritura a fórmula directa con offset (comentario en el diff):

```python
_start_dow     = dayofweek(purchase_date + 1)       # Spark: Dom=1..Sáb=7
offset         = (8 - start_dow) % 7                # días desde start hasta el primer domingo
_sundays_count = max(0, floor((total_days − 1 + 6 − offset) / 7))
```

Derivación: en una ventana de `ws = total_days − 1` días con primer domingo en posición `offset` (0-indexed), el número de domingos es `max(0, floor((ws − offset) / 7) + 1) = max(0, floor((ws + 6 − offset) / 7))`.

**Si vuelve a aparecer (timeliness 1 menos que legacy para algunos registros, patrón relacionado con day-of-week de purchase+1):** La señal es que la discrepancia aparece solo cuando `(total_days − 1) % 7 == (8 − start_dow) % 7`. Cualquier lógica `remaining >= days_to_next_sunday` tiene este off-by-one — usar `>` o bien la fórmula directa con offset.

**Estado:** Resuelto en código local (2026-06-09). Pendiente subir a S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py`) y re-ejecutar `glue-vi-calculate`.

---

## glue-vi-interchange: _apply_default() convertía NaN a cadena "nan" en columnas no-SPACE — filas excluidas de reglas válidas — RESUELTO

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `_apply_default`)
**Detectado:** 2026-06-09

**Síntoma:** Transacciones con valores NULL en columnas de condición que no pertenecen a `COLUMN_GROUP_SPACE` no matcheaban reglas válidas, cayendo en la regla fallback/default aunque todas las demás condiciones se cumplieran.

**Causa raíz:** La normalización de columnas no-SPACE era:

```python
temp = batch[condition_name].astype(str).str.strip()
temp = temp.mask(temp.str.len() == 0, "BLANK")
```

Pandas convierte `NaN → "nan"` con `.astype(str)`. Como `len("nan") == 3 ≠ 0`, el `.mask(len == 0, "BLANK")` no sustituye el valor — la columna queda con la cadena `"nan"`. Al contrastarla contra `valid_values` (e.g. `['Y', 'N', 'BLANK']`) no hay match → fila excluida. La intención del token `"BLANK"` es exactamente representar "campo vacío o ausente" — un NULL debe mapearse a `"BLANK"`, no a `"nan"`.

**Solución aplicada (2026-06-09):**

```python
temp = batch[condition_name].fillna("").astype(str).str.strip()
temp = temp.mask(temp.str.len() == 0, "BLANK")
```

`fillna("")` antes de `astype(str)` garantiza: `NaN → "" → "" (strip) → len=0 → "BLANK"`.

**Si vuelve a aparecer (filas con campos NULL no matchean reglas donde deberían):** Verificar que no haya `astype(str)` directo sobre columnas con nulls antes de normalizar. El patrón seguro es siempre `fillna("").astype(str).str.strip()` — tanto en `_apply_default` como en cualquier función nueva de normalización de condiciones.

**Estado:** Resuelto en código local (2026-06-09). Pendiente subir a S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py`) y re-ejecutar `glue-vi-interchange`.

---

## glue-vi-interchange: content_hash se perdía en el Parquet ITX por mapInPandas — RESUELTO (pendiente validar tras re-run)

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `evaluate_interchange_fees`)
**Detectado:** 2026-06-08

**Síntoma:** El usuario reportó que `content_hash` no aparecía en el Parquet de interchange (`itx.parquet`), aunque la columna sí figura en `interchange_cols` (lista de columnas finales de `process_output`).

**Causa raíz:** `evaluate_interchange_fees()` usa `mapInPandas()`, que **reemplaza por completo el schema** del DataFrame — cualquier columna no declarada explícitamente en `OUTPUT_COLS` y `output_schema` se descarta silenciosamente, sin error. `content_hash` SÍ llega como columna de entrada (viene de `cln_df`/`merged`, propagado desde transform→clean→calculate — ver `decisions.md` → "Por qué se agrega content_hash..."), pero `OUTPUT_COLS`/`output_schema` no lo declaraban, así que `yield result_pdf[OUTPUT_COLS]` lo eliminaba antes de que existiera en `result`. Luego `existing_cols = [c for c in interchange_cols if c in result.columns]` lo filtraba sin avisar — el job terminaba en SUCCESS, conteo correcto, pero sin la columna.

**Solución aplicada (2026-06-08):**
1. Agregado `"content_hash"` como primer elemento de `OUTPUT_COLS` (línea ~535)
2. Agregado `StructField("content_hash", StringType(), True)` como primer campo de `output_schema` (línea ~584)

**Si vuelve a aparecer (columna ausente en el Parquet final pese a estar en la lista de columnas finales):** sospechar de un `mapInPandas`/`applyInPandas` intermedio que reemplaza el schema — verificar que la columna esté declarada tanto en la lista de salida del iterador como en el `StructType` del schema, no solo en la selección final.

**Estado:** Resuelto en código y subido a S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py`, 2026-06-08). Pendiente re-ejecutar `glue-vi-interchange` y validar que `content_hash` aparece como primera columna del `itx.parquet` resultante.

---

## glue-vi-interchange: _apply_default() destruía el token "Space" (espacio literal) — transacciones GR caían en regla fallback — RESUELTO (pendiente validar tras re-run)

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `_apply_default`)
**Detectado:** 2026-06-08

**Síntoma:** Transacciones de Grecia (GR) con `acceptance_terminal_indicator` = espacio literal (`' '`) no matcheaban la regla `intelica_id=39` ("GR SECURE CR", criterio `acceptance_terminal_indicator='Space,9'`) — caían en la regla fallback/default `intelica_id=63` ("GR NON-SEC CR", `program_default='Y'`).

**Causa raíz:** En `_apply_default()`, dentro del loop que parsea `value_list` había un `value = value.strip()` extra que **no existe** en la versión validada del prototipo local (`tst_files/interchange_local.py` → `_apply_condition_default`). Para el criterio `"Space,9"`:
- Tras `replace("SPACE", " ")` + `split(",")`: `[' ', '9']`
- Con el `.strip()` extra: `' '` se convierte en `''` → `valid_values = ['', '9']`
- Como `acceptance_terminal_indicator` está en `COLUMN_GROUP_SPACE` (su valor de transacción se conserva como `' '` literal, sin normalizar/strip), el filtro `_normalized.isin(valid_values)` excluye toda transacción con `' '` porque `' ' not in ['', '9']`

**Cómo se detectó:** Comparación línea por línea de `_apply_default` (Glue) vs `_apply_condition_default` (local) — la única diferencia relevante era ese `.strip()` extra. Se confirmó vía regex sobre `tst_files/reference_data/visa_rules.parquet` que **ningún criterio real contiene comas seguidas de espacio** — el `.strip()` no tenía caso de uso legítimo, era código incidental que introdujo la regresión.

**Validación contra producción:** En el operational `D44C4427AED04C1E078AA86B275060FA.parquet` (jurisdiction_assigned=GR, 206,718 filas), 21,085 transacciones con `acceptance_terminal_indicator=' '` cayeron en la regla fallback 63. De ellas, **524 cumplían absolutamente TODAS las demás condiciones de la regla 39** (transaction_code, transaction_code_qualifier, account_funding_source, product_id, authorization_code, timeliness, pos_environment_code, pos_terminal_capability, pos_entry_mode, cardholder_id_method, authorization_response_code, reimbursement_attribute) — prueba directa de mala clasificación (`fee_descriptor='GR NON-SEC CR'` en vez de `'GR SECURE CR'`) causada únicamente por este bug.

**Solución aplicada (2026-06-08):** Eliminado `value = value.strip()` (línea 300) — alineando `_apply_default` con el comportamiento ya validado de `_apply_condition_default` del prototipo local.

**Si vuelve a aparecer (criterios "Space"/espacio literal no matchean):** verificar que ningún `.strip()` o normalización adicional se aplique a los valores de `value_list` después del `replace("SPACE", " ")` — el espacio literal debe sobrevivir intacto hasta el `isin()`.

**Estado:** Resuelto en código y subido a S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py`, 2026-06-08). Pendiente re-ejecutar `glue-vi-interchange` y validar que las transacciones GR con `acceptance_terminal_indicator=' '` que cumplen el resto de condiciones de la regla 39 ahora obtienen `interchange_intelica_id=39` (antes: 63).

---

## glue-vi-calculate: load_visa_ardef() vaciaba el ARDEF por to_date() sin formato — campos ARDEF quedaban 100% null — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py` (función `load_visa_ardef`)
**Detectado:** 2026-06-06

**Síntoma:** `calculate.parquet` se generaba correctamente (mismo Nº de filas que `clean.parquet`) pero los 10 campos derivados del cruce con ARDEF (`ardef_country`, `product_id`, `funding_source`, `b2b_program_id`, `fast_funds`, `nnss_indicator`, `product_subtype`, `technology_indicator`, `travel_indicator`, `issuer_country`) salían **100% null**.

**Causa raíz:** `effective_date` en `visa_ardef/data.parquet` viene como string en formato `yyyyMMdd` (ej. `'20131018'`, las 1,710,400 filas), pero el código llamaba `F.to_date(F.col("effective_date"))` **sin especificar formato**. `to_date()` sin formato espera ISO `yyyy-MM-dd`, así que devuelve `NULL` para el 100% de las filas. El filtro posterior `effective_date <= file_date` descarta entonces TODO el ARDEF (queda vacío), y el join produce 100% nulls en los campos derivados.

Adicionalmente había un pre-filtro (antes de convertir a `DateType`) que comparaba `effective_date` (string `yyyyMMdd`) directamente contra `file_date_str` (string `yyyy-MM-dd`) — comparación lexicográfica de formatos distintos, también incorrecta (cualquier dígito `'0'-'9'` > `'-'` en ASCII, así que fechas del mismo año del archivo se excluían incorrectamente).

**Cómo se detectó:** Replicando `load_visa_ardef()` + el range join en pandas (`tst_files/debug_ardef_join.py`) y comparando **valor a valor** (alineado por `record`, no por posición — Spark reordena filas) contra `calculate.parquet`. El join local daba 100% match contra los 553,929 rangos ARDEF válidos; el `calculate.parquet` real tenía 100% null en esos campos — 0% de coincidencia, confirmando que el ARDEF llegaba vacío al job real.

**Solución aplicada (2026-06-06):**
1. `F.to_date(F.col("effective_date"))` → `F.to_date(F.col("effective_date"), "yyyyMMdd")` — mismo patrón ya usado correctamente en `glue/scripts/mastercard/calculate/calculate.py:826-829` (`F.to_date(_fdt_str, "yyMMdd")` / `"yyyyMMdd"` según longitud).
2. Se eliminó el pre-filtro de strings con formatos distintos — el filtro real ya existe después de convertir ambas fechas a `DateType` (paso 3 de la función), que es format-agnostic y correcto.

**Si vuelve a aparecer (campos ARDEF en null):** Verificar primero que `ardef.count()` después de `load_visa_ardef()` no sea 0 o anormalmente bajo (el log `"ARDEF loaded: {count} valid ranges..."` lo reporta). Si es 0, sospechar de un cambio de formato en `effective_date`/`valid_until` del `data.parquet` de referencia — confirmar el formato real con una muestra antes de tocar el `to_date()`.

**Estado:** Resuelto — pendiente re-ejecutar `glue-vi-calculate` para regenerar `calculate.parquet` y validar con `debug_ardef_join.py` que el match sube a ~100%.

---

## mc-interpreter: mensaje IPM con DE_55 corrupto desincronizaba el stream y abortaba el archivo completo — RESUELTO

**Archivo:** `lambdas/mastercard/interpreter/src/handler.py` (función `read_len_prefixed_messages_variable`)
**Detectado:** 2026-06-10

**Síntoma:** Procesando `tst_files/T112T0.2026-01-06-13-11-32.001` (EBGR, encoding=cp500, need_unblock=TRUE), la lectura se detenía en el mensaje #26275 — el #26276 nunca se leía. En la versión previa del handler esto producía un `KeyError: 15` no controlado que tumbaba el generador completo, perdiendo TODOS los bloques ya procesados del archivo (porque `finalize_writers`/`fs.upload_tmp_outputs` solo corren si el generador termina sin excepción).

**Causa raíz:** El mensaje #26275 tiene un `DE_55` (ICC/EMV, longitud variable con prefijo de 3 dígitos) cuyo prefijo declara `length=120`, pero el contenido real son **118 bytes** — anomalía puntual del archivo fuente (confirmado byte a byte con `tst_files/debug_mc_interpreter_de55.py`: el patrón `f0 f1 f6 ...` ("016...") del siguiente DE_63 aparece en offset 1088, 2 bytes antes de la posición esperada 1090 si DE_55 fuera realmente 120 bytes). Confiar en el largo declarado desplaza la lectura del DE_63 dos bytes, su prefijo queda `"6 M"` → `int("6 M")` lanza `ValueError`.

El handler anterior tenía además **tres bugs que amplificaban esta única anomalía**:
1. `parameters[i]["fixed"]` con indexado directo — un DE no definido en `Parameters().getdataelements()` (p.ej. DE_15, que aparece en el bitmap del mensaje #26276 ya desincronizado) lanzaba `KeyError` **sin try/except**, abortando el generador completo.
2. `except Exception: de_len = 0` ante el `ValueError` de `int(raw_num.decode(encoding))` — no marcaba `parse_ok=False` ni hacía `break`, dejaba el stream permanentemente desincronizado y seguía produciendo filas basura.
3. El short-read en campos de longitud fija hacía `break` sin marcar `parse_ok=False` (inconsistente con la rama de longitud variable).

**Solución aplicada (2026-06-10):** Se portó el mecanismo de resync del sistema legacy (`tst_files/mcfiles.py` → `_resync_stream`), parametrizado por `encoding` (cp500/EBCDIC o latin-1/ASCII) en vez de hardcodear solo CP500 como el legacy:
- Nuevas funciones `_valid_mti_byte_patterns(encoding)` y `_resync_stream(stream, encoding, scan_limit=50000)`: escanean hacia adelante buscando 4 bytes de `record_length` plausible (`20 <= rl <= 65535`) seguidos de un MTI válido (`_RESYNC_MTIS = ("1240","1442","1644","1740")` — alineado con `subdir_for_mti`, a diferencia del legacy que usaba `{1240,1644,1440,1740}`).
- En el loop de parseo de DEs: `parameters.get(i)` (sin KeyError) y `parse_ok=False; break` en **cualquier** falla (DE no definido, `int()` inválido, short-read fijo o variable) — antes solo el short-read variable lo marcaba.
- Si `parse_ok=False` tras el loop: se llama a `_resync_stream`. Si encuentra punto de resync, se descarta el mensaje (no se hace `yield`, no se incrementa `msg_no`) y se continúa el `while`. Si no lo encuentra, se hace `break` (a diferencia del `on_error=True` del legacy que descartaba todo el archivo) — esto preserva los bloques ya procesados via `finalize_writers`/`upload_tmp_outputs` en `interpretate_msg`.

**Validación (2026-06-10):** `tst_files/debug_mc_interpreter_resync_test.py` — la lectura completa el archivo entero (422,734 mensajes, hasta el último byte del stream, MTI 1644 final), con `parse_ok=True` en el 100% de las filas yieldeadas. Se descartaron 2 mensajes corruptos consecutivos (offsets 18970881 y 18974467, ambos con `record_length` plausible y MTI 1240 detectado tras el resync), confirmando que la corrupción del archivo no era un caso aislado de 1 mensaje sino una zona con 2 mensajes afectados. Antes del fix, la lectura crasheaba (`KeyError: 15`) alrededor del mensaje #26276 y se perdía el archivo completo.

**Si vuelve a aparecer (lectura de un archivo MC se detiene antes del final, o `KeyError`/`ValueError` no controlado en `read_len_prefixed_messages_variable`):** Revisar el log `WARNING ... Mensaje corrupto descartado ... RESYNC exitoso/fallido`. Si el resync falla repetidamente cerca del mismo offset, sospechar de corrupción real del archivo fuente (no un bug del parser) — comparar con `debug_mc_interpreter_de55.py`/`de54.py` el rango de bytes alrededor del offset reportado.

**Estado:** Resuelto en código local (2026-06-10). Pendiente subir el handler actualizado al Lambda `lmbd-mc-interpreter` y validar end-to-end con `itx-mastercard-orchestrator`.

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
**Detectado:** 2026-06-02

**Problema:** `load_visa_ardef` descargaba el ARDEF filtrado al driver con `.toPandas()` y luego hacía deduplicación y eliminación de rangos solapados en pandas. Con archivos grandes, presionaba la heap del driver causando OOM → JVM caía → la siguiente llamada a `logger.info()` vía Py4J lanzaba `Py4JError: An error occurred while calling o<N>.info`.

**Solución aplicada (2026-06-02):** Migración completa a Spark — eliminado `toPandas()`, `import pandas as pd` y el parámetro `ardef_pd` de todas las firmas. Las operaciones de deduplicación y eliminación de solapamientos ahora usan `Window.partitionBy` + `row_number()` y `F.lag()`. El ARDEF nunca sale de los executors.

**Estado:** Resuelto. Si vuelve a aparecer `Py4JError` en este job, buscar en CloudWatch `Java heap space` o `ExecutorLostFailure` justo antes.

---

## glue-mc-interchange: filtra por file_id para no reprocesar ejecuciones anteriores

**Archivo:** `glue/scripts/mastercard/interchange/interchange.py`
**Detectado:** 2026-06-02 (implementación inicial)

**Problema (resuelto en la implementación):** Sin filtro por `file_id`, el job listaba TODOS los Parquets de la partición `file_type=X/date=YYYY-MM-DD` y reprocesaba archivos de ejecuciones anteriores del mismo día, actualizando su Last-Modified innecesariamente y potencialmente mezclando resultados de diferentes archivos fuente.

**Solución aplicada:** Filtrar los archivos listados por `stem_from_uri(path).upper().startswith(file_id.upper())` antes de procesarlos. Se aplica tanto a los archivos TXN (CLN) como a los CAL.

**Estado:** Resuelto. Comportamiento correcto en producción — cada ejecución del Step Function procesa únicamente sus propios archivos.

**Nota:** Este mismo patrón debe verificarse en `glue-vi-interchange` si alguna vez se presenta el mismo síntoma.

---

## glue-vi-calculate: timeliness debe ser LongType (bigint), NO IntegerType — HIVE_PARTITION_SCHEMA_MISMATCH

**Archivo:** `glue/scripts/visa/calculate/calculate.py`
**Detectado:** 2026-06-05

**Problema:** Si `calc_timeliness_draft` o `calc_timeliness_sms` usan `.cast(IntegerType())`, los Parquets nuevos escriben `int` (INT32). Los archivos existentes en S3 tienen `bigint` (INT64 / LongType — resultado natural de las aritméticas con `F.floor()` + `F.datediff()`). Al re-correr el crawler, la tabla queda con tipo `int` pero las particiones viejas siguen siendo `bigint`. Athena lanza:
```
HIVE_PARTITION_SCHEMA_MISMATCH: column 'timeliness' declared as type 'int',
but partition declared column 'timeliness' as type 'bigint'
```

**Solución aplicada (2026-06-05):** Usar `.cast(LongType())` para `timeliness` — tanto en `calc_timeliness_draft` como en `calc_timeliness_sms`. Todos los archivos (viejos y nuevos) quedan como `bigint`.

**Si vuelve a aparecer:** Verificar que el script en S3 use `LongType()`. Si hay particiones mixtas, editar la tabla en Glue catalog y forzar `bigint` manualmente antes de re-correr el crawler.

**Estado:** Resuelto.

---

## glue-vi-calculate: tipos explícitos en funciones de cálculo numérico

**Archivo:** `glue/scripts/visa/calculate/calculate.py`
**Detectado:** 2026-06-05

**Problema:** Sin `.cast()` explícito en columnas numéricas, Spark infiere tipos que el crawler de Glue detecta incorrectamente en Athena (e.g., `double` en lugar de `int`).

**Solución aplicada (2026-06-05):**

| Función | Cast aplicado |
|---------|--------------|
| `calc_business_transaction_type_draft` | `.cast(IntegerType())` |
| `calc_reversal_indicator_draft` | `.cast(IntegerType())` |
| `calc_reversal_indicator_sms` | `.cast(IntegerType())` |
| `calc_surcharge_amount` | `.cast(DoubleType())` + `F.lit(0.0)` |
| `calc_timeliness_draft` | `.cast(LongType())` — ver gotcha anterior |
| `calc_timeliness_sms` | `.cast(LongType())` — ver gotcha anterior |

**Regla:** Toda nueva función de cálculo numérico debe terminar con `.cast(TipoExplícito)`.

**Estado:** Resuelto.

---

## lmbd-vi-store: columnas enteras del CAL se escriben como double en operational — RESUELTO

**Archivo:** `lambdas/visa/store/src/handler.py`
**Detectado:** 2026-06-05

**Problema:** El crawler de Glue detectaba `timeliness` (y cualquier otra columna `LongType`/`IntegerType` del CAL que tuviera nulls) como `double` en la capa operational, en vez de `bigint`/`int`.

**Causa raíz:** `pq.read_table(...).to_pandas()` convierte automáticamente columnas INT64+nulls a `float64` (numpy no tiene tipo entero nullable). Al reconstruir la tabla con `pa.Table.from_pandas(merged)`, PyArrow infiere `double` desde `float64`. El `ParquetWriter` fija ese schema desde el primer batch y todos los archivos quedan como `double`.

**Solución aplicada (2026-06-05):** En `store_output`, el CAL se lee con `_read_parquet_arrow()` (devuelve `pa.Table`) en lugar de `_read_parquet_from_s3()`. Antes de convertir a pandas, se extrae `_cal_int_cols = {nombre: tipo}` para todas las columnas enteras del schema Arrow. En cada batch del loop, después de `pa.Table.from_pandas(merged)`, se restauran los tipos enteros con `merged_table.set_column(..., col.cast(atype))`. Arrow soporta `float64 null → int64 null` sin pérdida de datos.

**Por qué no se usó `use_nullable_dtypes=True`:** El layer usa una versión de PyArrow < 2.0 que no soporta ese parámetro.

**Si vuelve a aparecer:** Verificar que `_cal_int_cols` se construya correctamente antes del loop. Si hay nuevas columnas enteras en el CAL que queden como double, revisar que el schema del CAL Arrow tenga `is_integer(f.type) == True` para esas columnas.

**Estado:** Resuelto.

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
**Detectado:** 2026-06-08

**Síntoma 1 — `central_processing_date` / `account_reference_number_date` con valores ≈ -10 años:**
La lógica anterior `!YDDD` usaba "compute-then-correct": construir fecha tentativa (`decade + Y + DDD`) y si resultado > `file_date` → restar **10 años**. Para `campo='6004'` con `file_date=2026-01-03`: decodifica → `2026-01-04` > `2026-01-03` → resta 10 años → **2016-01-04**. Esto causaba `timeliness` del orden `-3653` días (≈ -10 años) en ~14% de los registros.

**Síntoma 2 — `purchase_date` retrocedía 1 año cuando debería conservar el año actual:**
La lógica anterior prepend año de `file_date`, y si resultado > `file_date` → restaba 1 año. Para `campo='0104'` (4-ene) con `file_date=2026-01-03`: `2026-01-04 > 2026-01-03` → restaba 1 año → **2025-01-04** (incorrecto). Causa: `purchase_date` usa formato MMDD y puede ser 1-2 días posterior al `file_date` dentro del mismo mes (el VIC procesa en días consecutivos). La spec Visa no prohíbe eso.

**Síntoma 3 — `conversion_date` aparecía con fecha futura (+1 año respecto al valor correcto):**
Misma lógica `!YDDD` sin restricción decodificaba `campo='6004'` como `2026-01-04`, cuando el correcto es **2025-01-04**. `conversion_date` es la fecha del archivo de tasas usado — una tasa del futuro es imposible según la spec Visa.

**Causa raíz:** La estrategia "compute-then-correct" (restar años si el resultado es futuro) es incorrecta para todos los casos. Los campos YDDD pueden legítimamente superar `file_date` en 1-2 días (VIC multi-día), y `purchase_date` MMDD no debe compararse por fecha completa sino solo por mes.

**Solución aplicada (2026-06-08):** Reescritura completa de `_parse_dates()` con tres estrategias derivadas de la spec Visa y del sistema legacy (adapters.py):

| Formato | Campos | Estrategia |
|---------|--------|-----------|
| `!YDDD` | `central_processing_date`, `account_reference_number_date` | `decade_of(file_date) + Y + DDD`, parsea `%y%j`. Sin corrección posterior — el resultado puede ser mayor a `file_date`. |
| `!YDDD_MAX` | `conversion_date` | Idéntico a `!YDDD` + cap: si resultado > `file_date` → restar 1 año. |
| `!MMDD` | `purchase_date` | Infiere año comparando **solo el mes**: `MM_campo <= MM_file_date` → mismo año; `MM_campo > MM_file_date` → año anterior. |

Todos los formatos: `'0000'` → `file_date` (proxy para evitar `NaT` en cálculos de `timeliness`).

**DynamoDB actualizado (2026-06-08):** `itl-0004-itx-dev-dynamo-visa_fields-02`, registro `type_record=draft / column_name=conversion_date` → `date_format` cambiado de `!YDDD` a `!YDDD_MAX`.

**Esquema real de claves de `visa_fields-02`:** HASH=`type_record`, RANGE=`column_name` (la documentación en CLAUDE.md decía `field_id` — error de documentación; la tabla tiene un GSI `type-record-index` que el código usa para queries).

**Validación completa vs PostgreSQL legacy (file_date=2026-01-03, 553,929 registros BASEII):**
```
purchase_date            !MMDD        2026-01-04    52502    52502  OK
purchase_date            !MMDD        2026-01-03    92346    92346  OK
conversion_date          !YDDD_MAX    2025-01-04    86519    86519  OK
conversion_date          !YDDD_MAX    2025-01-05   106026   106026  OK
central_processing_date  !YDDD        2026-01-04    86850    86850  OK
central_processing_date  !YDDD        2026-01-05   109105   109105  OK
account_ref_number_date  !YDDD        2026-01-04    77178    77178  OK
account_ref_number_date  !YDDD        2026-01-05    12607    12607  OK
```
0 nulls en todos los campos, 100% coincidencia con legacy.

**Si vuelven a aparecer fechas con año incorrecto en campos de fecha Visa:**
- `timeliness` ≈ ±3650 → alguna copia antigua del handler con la lógica "restar 10 años" — verificar que el deployment apunta al código correcto
- `purchase_date` un año atrás → revisar que la comparación sea solo por mes (`src_month > reference_date.month`), no por fecha completa
- `conversion_date` un año adelante → verificar `date_format=!YDDD_MAX` en DynamoDB y que el código aplique `future_mask`

**Estado:** Resuelto — `handler.py` subido al Lambda `lmbd-vi-clean` por el usuario (2026-06-08). Script de validación: `tst_files/debug_clean_dates.py`.

---

## Athena HIVE_BAD_DATA: columnas ARDEF (ardef_country, etc.) BINARY en Parquet vs integer en partición — RESUELTO

**Tabla:** `itl_0004_itx_dev_02_glue_database_operational_ebgr_visa.baseii_drafts`, partición `file_type=IN/date=2026-01-15`
**Detectado:** 2026-06-10

**Síntoma:** Athena lanzaba `HIVE_BAD_DATA: Malformed Parquet file. Field ardef_country's type BINARY in parquet file ... is incompatible with type integer defined in table schema [...]` al consultar `baseii_drafts`.

**Causa raíz:** Antes del fix de ARDEF (2026-06-06, ver gotcha "load_visa_ardef() vaciaba el ARDEF..."), los 10 campos derivados del cruce ARDEF (`ardef_country`, `product_id`, `b2b_program_id`, `fast_funds`, `nnss_indicator`, `product_subtype`, `technology_indicator`, `travel_indicator`, `funding_source`, `issuer_country`) salían 100% null en `calculate.parquet`. Con una columna 100% null, el crawler de Glue no puede inferir el tipo real desde los datos y los tipó como `int`. Ese tipo quedó grabado en la **metadata de la partición** `date=2026-01-15` (cada partición guarda su propia copia del schema al momento del crawl).

Tras el fix de ARDEF, el archivo de esa partición se regeneró con valores reales (`'US'`, `'GR'`, etc. — alfa-2, ver `tst_files/ardef.parquet` columna `ardef_country` dtype=object/string). El Parquet físico ahora tiene esas columnas como `BINARY` (string), pero la partición en el catálogo seguía con `int` porque no se había vuelto a crawlear desde el fix → choque schema-partición vs Parquet real.

El schema a **nivel de tabla** (`baseii_drafts`) ya estaba en `string` (otras particiones sí se habían re-crawleado después del fix) — solo esta partición específica quedó "congelada".

**Solución aplicada (2026-06-10):** Re-correr el crawler `itl_0004_itx_dev_02_glue_crawler_operational_ebgr_visa` (tiene `SchemaChangePolicy.UpdateBehavior=UPDATE_IN_DATABASE`). Verificado con `aws glue get-partition` antes/después: los 10 campos pasaron de `int` → `string` en la partición `date=2026-01-15`, coincidiendo con el Parquet y con el schema de tabla.

**Si vuelve a aparecer (`HIVE_BAD_DATA ... type BINARY ... incompatible with type integer/double/etc.` en cualquier tabla operational/staging):**
1. Identificar la partición exacta del error (`file_type=X/date=YYYY-MM-DD`) y la columna afectada.
2. `aws glue get-partition --database-name <db> --table-name <tabla> --partition-values "<file_type>" "<date>" --query "Partition.StorageDescriptor.Columns[?Name=='<col>']"` — comparar contra `aws glue get-table ... --query "Table.StorageDescriptor.Columns"` (schema de tabla).
3. Si difieren, re-correr el crawler correspondiente (`operational_ebgr_visa`, `staging_ebgr_visa`, etc. — todos tienen `UPDATE_IN_DATABASE`) y volver a comparar.
4. Causa típica: una columna que en algún momento fue 100% null (por un bug ya corregido) y el crawler le asignó un tipo "por defecto" que no coincide con el tipo real una vez que la columna empieza a tener datos.

**Estado:** Resuelto. Verificado también `staging_ebgr_visa` (2026-06-10) tras re-crawl: `400_baseii_cal_drafts` (tabla + las 3 particiones de `date=2026-01-15`, incluyendo el mismo file_id `57E3114D54623997062C63DA9CAD6BA7.parquet` del error original) ya estaba en `string` — sin mismatch. `500_baseii_itx_drafts` no contiene estas 10 columnas (el output de interchange no las propaga; vuelven a aparecer recién en `operational/baseii_drafts` vía el merge CAL+CLN+ITX de `lmbd-vi-store`). No se encontraron otros casos afectados.
