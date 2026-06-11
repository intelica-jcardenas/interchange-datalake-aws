# Ejecución manual de pasos del pipeline (debugging)

Contexto: para depurar el pipeline es mucho más rápido ejecutar cada paso a mano que volver a subir el archivo al S3 landing y esperar que el router + Step Functions arranquen todo desde cero.

---

## Prerequisito — autenticación AWS

```powershell
aws sso login --profile itx-dev
$env:AWS_PROFILE = "itx-dev"   # opcional, evita pasar --profile en cada comando
```

---

## Organización de tst_files/ (convención)

`tst_files/` está en `.gitignore` (línea `tst_file*`) — es el scratch space local de debugging, no se versiona. Para que no vuelva a acumularse desordenado (limpieza grande hecha el 2026-06-11, ver "Sesión 2026-06-11" → Paso 2), todo archivo nuevo debe terminar en una carpeta, no suelto en la raíz de `tst_files/`.

**Carpetas existentes y su propósito:**

| Carpeta | Contenido |
|---------|-----------|
| `glue_args/` | `.txt` con argumentos pegados de un run + `.json` generado por `generate_glue_args.py`, por job (`vi-calculate-run-*`, `vi-report-run-*`, `glue-test1-run-*`, etc.) |
| `reprocessing/` | Scripts de reproceso masivo (`reprocess_vi_*.py`) + sus logs `.jsonl` |
| `debug_scripts/` | Scripts de debug/validación reutilizables (escanear schemas, comparar outputs, validar campos nuevos) |
| `reference_data/` | Parquets/CSVs de tablas de referencia descargados de `s3-reference` para inspección local (`country_data`, `visa_rules`, `visa_bin_products`, etc.) |
| `reports/` | Outputs de `glue-test-1`/reportes y sus comparativos contra el legacy (CSV/Parquet) |

**Al terminar de usar un archivo nuevo (subido o generado durante una sesión):**

1. **Encaja en una carpeta existente** → moverlo ahí directamente (ej. un nuevo `vi-interchange-run-args.json` → `glue_args/`; un nuevo `validate_X.py` → `debug_scripts/`).
2. **No encaja en ninguna** (ej. debugging de un tema nuevo: payloads de Lambda, dumps de Mastercard, archivos IPM de prueba) → crear una carpeta nueva con nombre descriptivo del tema (ej. `mc_interpreter/`, `payloads/`) y agregarla a la tabla de arriba.
3. Si un script tiene rutas hardcodeadas (`LOG_PATH`, inputs/outputs), actualizarlas al moverlo — igual que se hizo en la reorganización del 2026-06-11.

**Eliminación de archivos que ya no se usarán:**

- Cuando un gotcha pasa a **RESUELTO Y VALIDADO** en `gotchas.md`, los archivos de `tst_files/` que sirvieron solo para esa investigación (dumps de datos, parquets intermedios, scripts de un solo uso) son candidatos a borrar — igual que los 23 archivos (~478MB) eliminados en la sesión 2026-06-11.
- **Antes de borrar:** verificar que ningún archivo en `gotchas.md`/`decisions.md`/memoria de usuario lo referencie como algo a re-ejecutar (los scripts reutilizables de `debug_scripts/` y `reprocessing/` casi nunca se borran). Las referencias a archivos ya borrados en notas de sesiones históricas no se actualizan — quedan como registro de lo que se hizo en su momento.
- Mantener: scripts genéricos reutilizables (`generate_glue_args.py`, `scan_nulltype_columns.py`, `compare_get_transaction.py`, etc.) y datos de referencia que se siguen usando para validar (`reference_data/`).

---

## Flujo de trabajo para Glue Jobs

### 1. Preparar los argumentos

Pegar los argumentos de la ejecución (copiados desde el payload del Step Function o desde un run anterior) en:

```
tst_files/glue_args/vi-calculate-run-test.txt   # o el .txt que corresponda al job
```

Formato del archivo: clave y valor en líneas alternas, sin separadores:
```
--content_hash
D44C4427AED04C1E078AA86B275060FA
--client_id
EBGR
...
```

### 2. Generar el JSON de argumentos

```powershell
python tst_files/glue_args/generate_glue_args.py
# genera tst_files/glue_args/vi-calculate-run-args.json

# con paths custom:
python tst_files/glue_args/generate_glue_args.py mi_args.txt mi_args.json
```

### 3. Lanzar el job

```powershell
aws glue start-job-run `
  --profile itx-dev `
  --job-name itl-0004-itx-dev-intchg-02-glue-vi-calculate `
  --arguments "file://tst_files/glue_args/vi-calculate-run-args.json"
# devuelve: { "JobRunId": "jr_..." }
```

### 4. Verificar estado del job

```powershell
aws glue get-job-run `
  --profile itx-dev `
  --job-name itl-0004-itx-dev-intchg-02-glue-vi-calculate `
  --run-id jr_XXXX `
  --query "JobRun.{State:JobRunState,Error:ErrorMessage,Start:StartedOn}" `
  --output table
```

---

## Nombres reales de los Glue Jobs

| Job | Nombre AWS |
|-----|-----------|
| vi-calculate | `itl-0004-itx-dev-intchg-02-glue-vi-calculate` |
| vi-interchange | `itl-0004-itx-dev-intchg-02-glue-vi-interchange` |
| mc-calculate | `itl-0004-itx-dev-intchg-02-glue-mc-calculate` |
| mc-interchange | `itl-0004-itx-dev-intchg-02-glue-mc-interchange` |

---

## Crawlers

### Lanzar crawler

```powershell
aws glue start-crawler `
  --profile itx-dev `
  --name itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa
```

Sin output = arrancó correctamente.

### Verificar estado del crawler

```powershell
aws glue get-crawler `
  --profile itx-dev `
  --name itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa `
  --query "Crawler.{State:State,LastStatus:LastCrawl.Status,Start:LastCrawl.StartTime}" `
  --output table
```

Estados posibles: `READY` (idle), `RUNNING`, `STOPPING`.

### Nombres reales de los crawlers

| Crawler | Nombre AWS |
|---------|-----------|
| Staging EBGR VISA | `itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa` |

---

## Lambdas (ejecución directa)

```powershell
# Invocación sync (espera resultado):
aws lambda invoke `
  --profile itx-dev `
  --function-name itl-0004-itx-dev-intchg-02-lmbd-vi-calculate `
  --payload "file://tst_files/payload.json" `
  --cli-binary-format raw-in-base64-out `
  response.json
cat response.json

# Invocación async (fire-and-forget):
aws lambda invoke `
  --profile itx-dev `
  --invocation-type Event `
  --function-name itl-0004-itx-dev-intchg-02-lmbd-vi-transform `
  --payload "file://tst_files/payload.json" `
  --cli-binary-format raw-in-base64-out `
  response.json
```

---

## Verificar S3 (datos presentes antes de lanzar el siguiente paso)

```powershell
# Listar lo que hay en staging para un cliente/marca/capa:
aws s3 ls s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/VISA/ --profile itx-dev

# Verificar que existe el parquet de un file_id concreto:
aws s3 ls "s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/VISA/400_baseii_cal_drafts/file_type=IN/date=2026-01-03/" --profile itx-dev
```

---

## Verificar tablas en Glue catalog

```powershell
aws glue get-tables `
  --profile itx-dev `
  --database-name itl_0004_itx_dev_02_glue_database_staging_ebgr_visa `
  --query "TableList[].{Name:Name,Updated:UpdateTime}" `
  --output table
```

---

## Sesión de debugging 2026-06-06 — lo que se ejecutó

**Job:** `glue-vi-calculate` para EBGR / VISA / IN / 2026-01-03

- `file_id`: `93BF199C85D2DF243AFDABEE5572E8C0`
- `content_hash`: `D44C4427AED04C1E078AA86B275060FA`
- `JobRunId`: `jr_3cebca36e4e90a00381cdf8bd0a3e578a69314bf7683e58de881a33bbed62033`
- Resultado: SUCCESS

**Crawler:** `itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa`
- Lanzado inmediatamente después del calculate
- Resultado: RUNNING al momento de guardar (pendiente confirmar SUCCEEDED)

**Archivos de soporte creados:**
- `tst_files/glue_args/vi-calculate-run-test.txt` — argumentos del job en texto plano
- `tst_files/glue_args/vi-calculate-run-args.json` — JSON generado para el CLI
- `tst_files/glue_args/generate_glue_args.py` — script que convierte txt → json

---

## Sesión de debugging 2026-06-06 (cont.) — bug ARDEF en calculate, fix y re-deploy

**Hallazgo:** El `calculate.parquet` generado en la sesión anterior tenía los 10 campos derivados de ARDEF en 100% null (`ardef_country`, `product_id`, `funding_source`, `b2b_program_id`, `fast_funds`, `nnss_indicator`, `product_subtype`, `technology_indicator`, `travel_indicator`, `issuer_country`).

**Causa:** `load_visa_ardef()` parseaba `effective_date` (formato `yyyyMMdd`) con `F.to_date()` sin formato explícito → devolvía `NULL` para el 100% de las filas → ARDEF quedaba vacío tras el filtro de fechas → join sin matches. Detalle completo en `gotchas.md` → "glue-vi-calculate: load_visa_ardef() vaciaba el ARDEF...".

**Fix aplicado:** `F.to_date(F.col("effective_date"), "yyyyMMdd")` + eliminación de un pre-filtro de strings con formatos de fecha incompatibles.

### Subir el script corregido al S3 del Glue job

`sync-glue.ps1` solo descarga (AWS → repo). Para subir un script editado localmente de vuelta a AWS, usar `aws s3 cp` directo al `ScriptLocation` que figura en `glue/scripts/<marca>/<job>/config.json` (campo `Job.Command.ScriptLocation`):

```
s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py
```

```powershell
aws s3 cp `
  glue/scripts/visa/calculate/calculate.py `
  s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py `
  --profile itx-dev
```

El siguiente `start-job-run` usará automáticamente la versión recién subida — no requiere ningún paso adicional de "deploy" o invalidación de caché.

### Re-ejecutar el job con los mismos argumentos de la corrida anterior

```powershell
aws glue start-job-run `
  --profile itx-dev `
  --job-name itl-0004-itx-dev-intchg-02-glue-vi-calculate `
  --arguments "file://tst_files/glue_args/vi-calculate-run-args.json"
```

**Resultado de esta sesión (2026-06-06):**
- `JobRunId`: `jr_a9f5bf312cfbf14dd2131d7e7ca275cf2f34e099e15a2e315e6cc291f8253e96`
- Resultado: **SUCCEEDED**

### Lanzar el crawler para refrescar el catálogo con el nuevo Parquet

```powershell
aws glue start-crawler `
  --profile itx-dev `
  --name itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa
```
(sin output = arrancó correctamente; lanzado tras confirmar el `calculate` en SUCCEEDED)

### Validar el fix

1. Descargar el nuevo `calculate.parquet` generado a `tst_files/` (sobrescribiendo el anterior)
2. Re-correr `python tst_files/debug_ardef_join.py` — el PASO 5 debe mostrar ~100% de match en los 10 campos ARDEF (antes: 0%, todo null)

---

## Sesión de debugging 2026-06-08 — bugs en glue-vi-interchange (content_hash perdido + acceptance_terminal_indicator "Space"), fix y subida a S3

**Hallazgo 1 — `content_hash` ausente en el Parquet ITX:** `evaluate_interchange_fees()` usa `mapInPandas()`, que reemplaza el schema completo del DataFrame; `content_hash` no estaba declarado en `OUTPUT_COLS` ni en `output_schema`, así que se descartaba silenciosamente aunque sí llegaba como columna de entrada (propagada desde clean/calculate vía `merged = cln_df.join(cal_df...)`). Detalle completo en `gotchas.md` → "glue-vi-interchange: content_hash se perdía en el Parquet ITX por mapInPandas".

**Hallazgo 2 — `acceptance_terminal_indicator` con criterio "Space" no matcheaba:** comparando `_apply_default()` (Glue) contra `_apply_condition_default()` (prototipo local en `tst_files/interchange_local.py`) se encontró un `value = value.strip()` extra que convertía el espacio literal `' '` en `''`, excluyendo transacciones GR con `acceptance_terminal_indicator=' '` de la regla `intelica_id=39` ("GR SECURE CR") y desviándolas a la regla fallback `63` ("GR NON-SEC CR"). Validado contra el operational `D44C4427AED04C1E078AA86B275060FA.parquet`: 524 transacciones GR cumplían TODAS las demás condiciones de la regla 39 y fueron mal clasificadas solo por este bug. Detalle completo en `gotchas.md` → "glue-vi-interchange: _apply_default() destruía el token Space".

**Fixes aplicados (2026-06-08) en `glue/scripts/visa/interchange/interchange.py`:**
1. `"content_hash"` agregado como primer elemento de `OUTPUT_COLS` y `StructField("content_hash", StringType(), True)` como primer campo de `output_schema` (función `evaluate_interchange_fees`)
2. Eliminado el `value = value.strip()` extra dentro del loop de `_apply_default()` (línea ~300)

### Subir el script corregido al S3 del Glue job

Mismo patrón que la sesión del fix de ARDEF (`calculate.py`, sección anterior). `ScriptLocation` del job `glue-vi-interchange` (campo `Job.Command.ScriptLocation` en `glue/scripts/visa/interchange/config.json`):

```
s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py
```

```powershell
aws s3 cp `
  glue/scripts/visa/interchange/interchange.py `
  s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py `
  --profile itx-dev
```

**Resultado de esta sesión (2026-06-08):** subida completada —
`upload: glue\scripts\visa\interchange\interchange.py to s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py`

El siguiente `start-job-run` de `itl-0004-itx-dev-intchg-02-glue-vi-interchange` usará automáticamente esta versión.

### Pendiente (próxima sesión)

1. Re-ejecutar `glue-vi-interchange` (mismo patrón txt→json→start-job-run que en `vi-calculate`, ver Pasos 1-4 al inicio de este documento) para el `file_id`/`content_hash` `D44C4427AED04C1E078AA86B275060FA`
2. Descargar el nuevo `itx.parquet` y validar:
   - `content_hash` aparece como **primera columna**
   - Las transacciones GR con `acceptance_terminal_indicator=' '` que cumplen el resto de condiciones de la regla 39 ahora obtienen `interchange_intelica_id=39` (`GR SECURE CR`) en vez de `63` (`GR NON-SEC CR`)

---

## Sesión de debugging 2026-06-08 — bug _parse_dates en lmbd-vi-clean (fechas YDDD/MMDD incorrectas)

**Hallazgo:** Tres de los cuatro campos de fecha en `clean.parquet` producían valores incorrectos — raíz en la lógica "compute-then-correct" de `_parse_dates()`.

| Campo | Bug | Ejemplo incorrecto | Correcto |
|-------|-----|--------------------|---------|
| `central_processing_date` | `!YDDD` restaba 10 años si resultado > file_date | `2016-01-04` | `2026-01-04` |
| `account_reference_number_date` | Mismo bug | `2016-01-04` | `2026-01-04` |
| `purchase_date` | `!MMDD` comparaba fecha completa vs solo mes | `2025-01-04` | `2026-01-04` |
| `conversion_date` | `!YDDD` sin cap → fecha futura sin corrección | `2026-01-04` | `2025-01-04` |

**Debugging:** Comparación de conteos agrupados por fecha contra PostgreSQL legacy usando `tst_files/debug_clean_dates.py` (sobre `tst_files/extract.parquet`, file_date=2026-01-03). Se leyó spec Visa (`tst_files/fechas.txt`) y adapters.py del sistema legacy para derivar la lógica correcta.

**Fix aplicado en `lambdas/visa/clean/src/handler.py`:** Reescritura completa de `_parse_dates()`:
- `!YDDD` → `decade_of(file_date) + Y + DDD`, sin corrección posterior
- `!YDDD_MAX` → igual que `!YDDD` + cap: si resultado > `file_date` → restar 1 año
- `!MMDD` → inferir año comparando solo el mes (`src_month > reference_date.month`)
- Todos los formatos: `'0000'` → `file_date`

**DynamoDB actualizado:**
```powershell
aws dynamodb update-item `
  --profile itx-dev `
  --table-name itl-0004-itx-dev-dynamo-visa_fields-02 `
  --key '{"type_record": {"S": "draft"}, "column_name": {"S": "conversion_date"}}' `
  --update-expression "SET date_format = :v" `
  --expression-attribute-values '{":v": {"S": "!YDDD_MAX"}}' `
  --return-values ALL_NEW
```

Nota: las claves reales de `visa_fields-02` son `type_record` (HASH) + `column_name` (RANGE).

**Resultado de esta sesión (2026-06-08):** handler.py subido al Lambda `lmbd-vi-clean` por el usuario — pendiente confirmar resultado en producción.

---

## Sesión de debugging 2026-06-09 — bug fillna(0.0) en glue-vi-interchange (fees zerados), fix y subida a S3

**Hallazgo:** Al comparar `sum(interchange_fee_amount)` por jurisdiction y source_currency contra el legacy PostgreSQL, se detectaron diferencias en jurisdicciones off-us EUR (−289 USD) e interregional JPY (+29 JPY). Tras descartar que la causa fuera el cálculo de timeliness (ya corregido) o el _apply_default NaN (ya corregido), se identificaron dos problemas:

**Bug 1 — `fillna(0.0)` en `fee_min`/`fee_cap` (RESUELTO):**
`process_pandas_partitions` aplicaba `.fillna(0.0)` a `interchange_fee_min` e `interchange_fee_cap`. Reglas sin cap/min definido tienen `NaN`; `fillna(0.0)` lo convierte a `0.0` → Spark lo recibe como valor real → `coalesce(0.0, +inf) = 0.0` → `least(fee_amount, 0.0) = 0` — todos los fees positivos de esas reglas quedaban en cero.

**Fix aplicado (2026-06-09):** Eliminado `.fillna(0.0)` de `interchange_fee_min` e `interchange_fee_cap` (solo se deja `.astype(float)`). NaN → NULL en Spark → coalesce(±inf) → sin restricción.

### Subir el script corregido al S3

```powershell
aws s3 cp `
  glue/scripts/visa/interchange/interchange.py `
  s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py `
  --profile itx-dev
```

**Resultado de esta sesión (2026-06-09):** subida completada. El siguiente `start-job-run` de `itl-0004-itx-dev-intchg-02-glue-vi-interchange` usará automáticamente esta versión.

**Bug 2 — Diferencia residual interregional JPY: intelica_id 1065 vs 1055 (PENDIENTE):**
La diferencia de −29.64 para interregional JPY (1 transacción, source_amount=20,220 JPY) es por rule matching incorrecto: legacy asigna 1065 "ATM AF JPN" (fee_fixed=0.50 USD, fee_currency=USD) pero el nuevo sistema asigna 1055 "ATM AF" (fee_fixed=0, fee_currency=None). Son reglas distintas con monedas distintas — la comparación numérica directa no tiene sentido. Requiere investigar qué condición en `visa_rules` diferencia ambas reglas y por qué no se aplica en el nuevo sistema.

**Bug 3 — Convención de exchange_value pendiente de verificar:**
El legacy aplica `exchange_value` sobre `source_amount` (resultado en fee_currency). El prototipo lo aplica sobre los componentes de la regla (resultado en source_currency). El usuario prefiere source_currency. Requiere verificar si `exchange_value` en S3 `exchange_rate/data.parquet` es `fee_ccy/source_ccy` (~1.08 para EUR→USD) o `source_ccy/fee_ccy` (~0.926). Verificar con:
```python
import pandas as pd
df = pd.read_parquet('tst_files/exchange_rate_data.parquet')  # descargar antes
print(df[(df['currency_from']=='EUR') & (df['currency_to']=='USD')][['exchange_value']].head())
```

### Pendiente (próxima sesión)

1. Re-ejecutar `glue-vi-interchange` con los mismos argumentos del file_id `D44C4427AED04C1E078AA86B275060FA`
2. Comparar `sum(interchange_fee_amount)` por jurisdiction/source_currency — verificar que off-us EUR ya no tiene diferencia de −289
3. Investigar condición diferenciadora entre reglas 1055 y 1065 en `visa_rules.parquet`
4. Verificar dirección del `exchange_value` en S3 reference

---

## Sesión de debugging 2026-06-10 — columnas NullType en operational baseii_drafts (message_reason_code, type_of_purchase), fix generalizado en lmbd-vi-store y reprocesamiento masivo

**Contexto:** `glue-test-1` (`get_transaction.py`) fallaba con `SchemaColumnConvertNotSupportedException` al leer `EBGR/VISA/baseii_drafts/file_type=IN/date=2026-01-0X/` (directorio completo, varios `file_id` por fecha). Primero en `message_reason_code` (Expected: string, Found: INT32), luego — tras un primer fix puntual — en `type_of_purchase`.

**Root cause:** columnas del CAL 100% null para ciertos `file_id` se degradan a `pa.null()` (NullType → INT32 en Parquet) durante el round-trip pandas/pyarrow en `lmbd-vi-store`. Otros archivos del mismo directorio tienen la columna como `string` real → Spark no puede leer el directorio con un schema único. Detalle completo en `gotchas.md` → "lmbd-vi-store: columnas NullType en operational rompen lectura de directorio completo con Spark".

**Fix aplicado y desplegado por el usuario:** generalización de `_cal_int_cols` → `_cal_dtype_map` en `lambdas/visa/store/src/handler.py` — restaura tanto `int64+nulls→float64` como `string-100%-null→NullType`.

### Paso 1 — Mapear content_hash → file_id

```powershell
aws dynamodb scan `
  --profile itx-dev `
  --table-name itl-0004-itx-dev-dynamo-file_control-02 `
  --filter-expression "content_hash = :h1 OR content_hash = :h2" `
  --expression-attribute-values '{":h1": {"S": "<content_hash_1>"}, ":h2": {"S": "<content_hash_2>"}}' `
  --query "Items[].{file_id:file_id.S, content_hash:content_hash.S, store_result:store_result.S}"
```
`store_result` (JSON) contiene `outputs[].cln_s3_key` para cada `output_type` (BASEII, VSS_110/120/130/140) — necesario para construir el payload de `lmbd-vi-store`.

### Paso 2 — Payload de reprocesamiento (un output_type por invocación)

```json
{
  "client_id": "EBGR",
  "file_id": "<file_id>",
  "brand": "VISA",
  "file_type": "IN",
  "file_date": "<YYYY-MM-DD>",
  "content_hash": "<content_hash>",
  "outputs": [
    {
      "output_type": "BASEII",
      "s3_key": "EBGR/VISA/300_baseii_cln_drafts/file_type=IN/date=<YYYY-MM-DD>/<content_hash>.parquet"
    }
  ]
}
```

```powershell
aws lambda invoke `
  --profile itx-dev `
  --function-name itl-0004-itx-dev-intchg-02-lmbd-vi-store `
  --payload "file://payload.json" `
  --cli-binary-format raw-in-base64-out `
  response.json
```

### Paso 3 — Escanear NullType en todo un directorio (sin descargar archivos completos)

`tst_files/debug_scripts/scan_nulltype_columns.py` — usa `pyarrow.fs.S3FileSystem` (credenciales del perfil `itx-dev`, region `eu-south-2`) + `pq.ParquetFile(...).schema_arrow` para leer solo el footer de cada Parquet bajo un prefijo S3, y reporta qué columnas tienen `pa.types.is_null(f.type) == True`. Ajustar `BUCKET`/`PREFIX` al cliente/marca/tipo a auditar.

```powershell
python tst_files/debug_scripts/scan_nulltype_columns.py
```

### Resultado de esta sesión (2026-06-10)

- Escaneo inicial de `EBGR/VISA/baseii_drafts/file_type=IN/` (56 archivos, 2026-01-01..2026-01-30): **54/56** con `type_of_purchase` en NullType, **27/54** además con `message_reason_code`.
- Reprocesados con `lmbd-vi-store` (output_type=BASEII, handler corregido): **56/56 SUCCESS** (2 ya habían sido reprocesados antes en la misma sesión, 4 para completar el rango 1-5 enero, 50 para el resto de fechas hasta el 30 de enero).
- Re-escaneo final: **0/56** con columnas NullType.
- `glue-test-1` relanzado para rango 2026-01-01..2026-01-05 (`report_suffix=20260105_tst`) → JobRunId `jr_b0e8b19c35c6128524a4bb5cd8f137096938c453fe60c9a003630bc22c5b732c` — pendiente confirmar resultado.

### Pendiente (próxima sesión)

1. Confirmar resultado de `jr_b0e8b19c35c6128524a4bb5cd8f137096938c453fe60c9a003630bc22c5b732c`.
2. Si SUCCESS, considerar correr el mismo escaneo (`scan_nulltype_columns.py`) sobre `SBSA` y `BTRLRO` (otra convención de paths: `BTRLRO/VI/...`) y sobre `vss_110/120/130/140` de EBGR, antes de generar reportes que cubran esos clientes/tipos.

---

## Sesión de debugging 2026-06-10 (cont.) — bug to_currency en glue-test-1, fix y re-run

**Contexto:** El run `jr_b0e8b19c35c6128524a4bb5cd8f137096938c453fe60c9a003630bc22c5b732c` (punto 1 del pendiente anterior) terminó `SUCCEEDED` pero sin generar reporte — ver gotcha "glue-test-1 (glue-vi-mc-reporting): load_exchange_rates() leía tabla incompleta y con columnas incorrectas" en `gotchas.md`.

### Cómo encontrar el ScriptLocation real de un Glue job

El nombre conceptual `glue-vi-mc-reporting` no es el nombre real desplegado. Para encontrarlo:

```powershell
aws glue get-jobs --profile itx-dev --query "Jobs[].Name" --output table
# -> itl-0004-itx-dev-intchg-02-glue-test-1 (entre otros glue-test-2/3/4)

aws glue get-job --profile itx-dev --job-name itl-0004-itx-dev-intchg-02-glue-test-1 `
  --query "Job.Command.ScriptLocation" --output text
# -> s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/get_transaction.py
```

### Verificar schema real de una fuente de referencia (sin descargar el Parquet completo)

`tst_files/debug_scripts/check_xrate_schema.py` (mismo patrón que `scan_nulltype_columns.py` — `pyarrow.fs.S3FileSystem` + `pq.ParquetFile(path, filesystem=fs).schema_arrow`, además `.read().to_pandas()` para inspeccionar valores de muestra de un solo archivo). Se usó para comparar:
- `exchange-rates/brand=Visa/exchange_date=2026-01-01/*.parquet` → columnas `currency_from, currency_to, currency_from_code, currency_to_code, exchange_value` (sin `exchange_date`, viene de la partición). No tenía fila `EUR→USD`.
- `exchange_rate/rate_date=2026-01-05/*.parquet` → mismas columnas + `brand` (`VISA`/`MasterCard`) + `year`/`month`. Sí tenía `VISA EUR→USD` (fila única, `exchange_value≈1.1766`).

### Subir el script corregido y re-ejecutar

```powershell
aws s3 cp glue/scripts/reports/get_transaction/get_transaction.py `
  s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/get_transaction.py `
  --profile itx-dev

# Recuperar los argumentos del run anterior para reusarlos:
aws glue get-job-run --profile itx-dev --job-name itl-0004-itx-dev-intchg-02-glue-test-1 `
  --run-id jr_b0e8b19c35c6128524a4bb5cd8f137096938c453fe60c9a003630bc22c5b732c `
  --query "JobRun.Arguments"

# Cambiar --report_suffix para no pisar el output del run anterior (que de todos modos no generó nada)
# y relanzar:
aws glue start-job-run --profile itx-dev --job-name itl-0004-itx-dev-intchg-02-glue-test-1 `
  --arguments file://tst_files/glue_args/glue-test1-run-args.json --query "JobRunId" --output text
```

**Resultado de esta sesión (2026-06-10):** script corregido subido. Relanzado con `report_suffix=20260105_tst2` → JobRunId `jr_ecbf44e09aa4db4cabceb597478ffc21b18b27a9b4dc02f7f020fe039c284c3d` — **pendiente confirmar resultado** (verificar `JobRunState=SUCCEEDED` y que esta vez sí exista output en `s3-analytics` para EBGR 2026-01-01..2026-01-05, sin el mensaje "No data... skipping").

### Pendiente (próxima sesión)

1. Confirmar resultado de `jr_ecbf44e09aa4db4cabceb597478ffc21b18b27a9b4dc02f7f020fe039c284c3d` — revisar `s3://itl-0004-itx-dev-intchg-02-s3-analytics/` para el output con sufijo `20260105_tst2`.
2. Si hay output, validar el contenido del reporte (31 columnas `FINAL_COLS`, `xr1_rate`/`xr2_rate` no nulos para filas con moneda distinta a EUR).
3. Pendiente del punto 2 de la sesión anterior: escanear NullType en `SBSA`/`BTRLRO`/`vss_110-140`.

---

## Sesión 2026-06-11 — nuevos campos en glue-vi-calculate (business_transaction_cycle, settlement_report_currency_code), reproceso masivo calculate+store EBGR enero 2026, re-crawl, limpieza tst_files

**Contexto:** Continuación de la sesión 2026-06-10. Se agregaron dos campos nuevos a `calculate.py` (detalle y lógica en `decisions.md` → "Por qué se agregaron business_transaction_cycle y settlement_report_currency_code"). Validados primero contra 1 archivo, luego reprocesados para todo enero.

### Paso 1 — Validación 1 archivo

Re-uso de `tst_files/glue_args/vi-calculate-run-args.json` (file_id=`93BF199C85D2DF243AFDABEE5572E8C0`, content_hash=`D44C4427AED04C1E078AA86B275060FA`, EBGR, 2026-01-03). Primer intento falló:
```
GlueArgumentError: the following arguments are required: --dynamodb_table_client
```
**Fix:** agregado `"--dynamodb_table_client": "itl-0004-itx-dev-dynamo-client-02"` al JSON de argumentos (la función `get_client_data()` ya existía en `calculate.py` y lo requiere para `settlement_report_currency_code`). Re-run → `jr_c8e0e02fd3b1c81d449fa9b7d77cae6816fc1cb137180dae719420557a2464fc` → SUCCEEDED (127s).

Luego se invocó `lmbd-vi-store` (BASEII) con el CAL regenerado y se validó `tst_files/reports/operational_validate.parquet` con `tst_files/debug_scripts/validate_new_fields.py`:
- `business_transaction_cycle`: int32, 0 nulls, distribución correcta por `draft_code`/`usage_code`
- `settlement_report_currency_code`: string, 0 nulls, 100% `"EUR"`

### Paso 2 — Limpieza de tst_files

Se identificaron y eliminaron 23 archivos (~478MB) ligados a gotchas ya RESUELTOS (datos de debug de NullType, ARDEF, fechas, etc.), manteniendo 11 archivos (268KB) reutilizables o ligados a pendientes (ATM JPY 1055/1065, exchange rate glue-test-1).

### Paso 3 — Reproceso masivo glue-vi-calculate (100 archivos)

Script: `tst_files/reprocessing/reprocess_vi_calculate.py`. Scope: `client_id=EBGR, brand_id=VI, file_type=IN, file_processing_date BETWEEN 2026-01-01 AND 2026-01-30, control_status=DONE` (scan DynamoDB `file_control-02`, paginado).

Por archivo, `--outputs` se construye desde `store_result.outputs[].cln_s3_key` (todos los output_types: BASEII + VSS_110/120/130/140 — `calculate.py` los procesa todos, a diferencia de interchange).

Respeta `MaxConcurrentRuns=50` del job (`itl-0004-itx-dev-intchg-02-glue-vi-calculate`) con margen de seguridad → límite efectivo 45. Loop: `get_job_runs` (paginado, `MaxItems=500`) para contar activos + estado de runs propios, lanza `start_job_run` mientras haya slots, poll cada 30s, `time.sleep(1.5)` entre starts para evitar throttling.

**Resultado:** `tst_files/reprocessing/reprocess_vi_calculate_log.jsonl` — **100/100 SUCCEEDED**, 0 fallos.

### Paso 4 — Reproceso masivo lmbd-vi-store (100 archivos)

Script: `tst_files/reprocessing/reprocess_vi_store.py`. Mismo scan DynamoDB. Por archivo arma **un solo payload** (no uno por output_type) con `outputs[]` = los 5 output_types (BASEII + VSS_110-140), cada uno `{output_type, s3_key=cln_s3_key}` — `lambda_handler` ya itera internamente sobre `outputs`, así que es **1 invocación Lambda = 1 archivo** (100 invocaciones totales, no 500).

`itl-0004-itx-dev-intchg-02-lmbd-vi-store` no tiene concurrencia reservada (pool compartido de la cuenta) y no escribe en DynamoDB — se usó `ThreadPoolExecutor(max_workers=20)` con `invoke(InvocationType="RequestResponse")` (timeout boto3 `read_timeout=910`, `retries={"max_attempts": 0}`).

**Resultado:** `tst_files/reprocessing/reprocess_vi_store_log.jsonl` — **100/100 SUCCESS**, 0 fallos.

### Paso 5 — Re-crawl staging + operational EBGR VISA

```powershell
aws glue start-crawler --profile itx-dev --name itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa
aws glue start-crawler --profile itx-dev --name itl_0004_itx_dev_02_glue_crawler_operational_ebgr_visa
```

Ambos `UpdateBehavior=UPDATE_IN_DATABASE` (mismo mecanismo que resolvió el caso ARDEF de 2026-06-10). Resultado: ambos `SUCCEEDED`. Confirmado en catálogo:
```
aws glue get-table --database-name itl_0004_itx_dev_02_glue_database_operational_ebgr_visa --name baseii_drafts \
  --query "Table.StorageDescriptor.Columns[?Name=='business_transaction_cycle' || Name=='settlement_report_currency_code']"
# business_transaction_cycle  -> int
# settlement_report_currency_code -> string
```

### Paso 6 — Script reprocess_vi_interchange.py (creado, NO ejecutado)

Mismo patrón que `reprocess_vi_calculate.py`, para `itl-0004-itx-dev-intchg-02-glue-vi-interchange` (`MaxConcurrentRuns=50`, límite efectivo 45). `interchange.py` requiere `client_id, file_id, file_type, file_date, staging_bucket, reference_bucket, outputs` (sin `dynamodb_table_client`/`content_hash`/`brand`); `outputs` solo BASEII/SMS (VSS se ignora internamente, se filtra antes de enviar).

**Por qué no se ejecutó:** las dos columnas nuevas no afectan reglas de interchange; correrlo ahora dejaría `500_baseii_itx_drafts` actualizado pero el `operational/baseii_drafts` (ya regenerado en el paso 4 con el ITX viejo) quedaría desincronizado — requeriría un segundo pase de `lmbd-vi-store` después. Queda listo para cuando haya un cambio que sí justifique reprocesar interchange.

### Pendiente (próxima sesión)

1. Continuar desarrollo de `glue-vi-mc-reporting` / `get_transaction.py` — confirmar resultado de `jr_ecbf...284c3d` (punto pendiente de la sesión 2026-06-10, ver arriba).
2. Si se necesita reprocesar interchange en el futuro, usar `tst_files/reprocessing/reprocess_vi_interchange.py` + segundo pase de `tst_files/reprocessing/reprocess_vi_store.py`.
4. Cuando esté disponible el nuevo método de extracción de tipo de cambio Visa, revisar si `load_exchange_rates()` debe cambiar de fuente nuevamente.
