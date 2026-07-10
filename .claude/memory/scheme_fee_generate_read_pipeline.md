# Scheme Fee — etapas de generate/read vs legacy y carpetas de s3-analytics

**Archivo temporal** — vive acá solo mientras se termina de desarrollar y validar Scheme Fee. Una vez cerrado, retirar (o fusionar lo que siga siendo relevante a `decisions.md`/`gotchas.md`) en vez de dejarlo indefinidamente.

Referencia viva (no cronológica, se actualiza in-place) de las etapas de `--mode generate`/`--mode read` de `glue-scheme-fee` comparadas contra `managment.py`/`getquery.py` del legacy, y de qué contiene cada carpeta de `s3-analytics/{cliente}/scheme_fee/`. El histórico de bugs/investigaciones/decisiones de diseño vive en la memoria de usuario `scheme_fee_job_design.md` (no versionada en este repo) — este archivo es solo el mapeo de etapas + estructura de carpetas, mantenido actualizado.

---

## Etapas de `--mode generate` vs legacy

| Etapa | `scheme_fee.py` (nuevo) | Legacy (`managment.py`/`getquery.py`) |
|---|---|---|
| 1. Lectura de transacciones | `read_operational()` por marca (VISA `baseii_drafts`, VISA `sms_messages` si `ENABLE_SMS`, MC `IPM_1240`/`IPM_1442`) leyendo Parquet de `s3-operational` | Queries separadas contra `operational.dh_visa_transaction_calculated_field_*` / `dh_mastercard_*` en PostgreSQL |
| 2. Terminal count (Grecia) | `compute_visa_terminal_counts()` — antes del transform principal | Igual, antes del INSERT principal |
| 3. Transform por marca/tipo | `transform_visa_baseii_scheme_fee()` (unifica ISS+ACQ), `transform_visa_sms_scheme_fee()`, `transform_mastercard_scheme_fee()` — cada una resuelve las ~33 columnas de detalle (funding_source, product_id, jurisdiction, business_transaction_type_id, currency_local_indicator, motoec_indicator, travel_program_indicator, greece_micropayment_indicator, key_entered_tpe, exchange_rate/report_amount) | `get_issuers_visa()`/`get_acquirer_visa()` (2 INSERTs separados por ISS/ACQ), `get_sms_visa()`, `get_transactions_mastercard()` |
| 4. Duplicado on-us | `_apply_duplicate_on_us()` (filter jurisdiction=on-us & ACQUIRING → union con copia ISSUING), condicional a `dup_on_us_visa`/`dup_on_us_mc` del cliente | `get_on_us_visa()`/`get_sms_on_us_visa()`/`get_on_us_mastercard()` — 3 INSERTs SQL condicionales |
| 5. Union de las 3 fuentes | `detail_df = unionByName(...)` | Todo ya vive en una sola tabla de detalle en Postgres (INSERTs acumulativos) |
| 6. Size ticket | `resolve_size_ticket()` — después del union, mismo momento relativo que legacy | `update_size_tickets()` — bulk UPDATE después de todos los INSERTs |
| 7. Agregación | `aggregate_to_report(detail_df)` — `groupBy(*GROUP_DIMS)` (20 dimensiones) + `COUNT(*)`/`SUM(report_amount)` | `get_insert_into_report_table()` — mismo GROUP BY, mismas 20 dimensiones, mismo orden |
| 8. Export a formato legacy | `build_legacy_export()` — mapea IDs (`bus_id`, `sch_id`, `txn_scp_id`, `txn_typ_id`, `mct_ctry_id`, `fnd_src_id`), asigna `app_id` secuencial, escribe CSV a `s3-scheme-fee/OUT/{cliente}/` | `get_insert_into_report_legacy_table()` — mismos mapeos, `app_id` con `ROW_NUMBER() OVER (ORDER BY ...)` determinístico (diferencia conocida: la versión nueva no es determinística entre corridas — `report_pdf = report_df.toPandas()` sin sort explícito antes de asignar `app_id`; sin impacto dentro de un mismo ciclo generate→read) |
| 9. Persistencia de estado | `detail_df.write` → `state/{mes}/detail/`; `report_pdf` (+ `app_id`) → `state/{mes}/report/`; `summary.json` | Todo queda en las tablas Postgres — no hay "estado" separado, la tabla ES el estado |

`get_exchange_rate_calculation()` (bulk UPDATE de FX) y 6 queries de limpieza de NULLs (`update_jurisdiction`, etc.) están definidas en legacy pero **nunca se ejecutan** (código muerto, confirmado por grep) — no hace falta replicarlas.

## Etapas de `--mode read` vs legacy

| Etapa | `scheme_fee.py` (nuevo) | Legacy |
|---|---|---|
| 1. Validación previa | Requiere `summary.json` existente (si no, error "correr generate primero") | Requiere que el INSERT de detalle/reporte ya se haya hecho |
| 2. Lectura del CSV de vuelta | `pd.read_csv()` del archivo en `s3-scheme-fee/IN/{cliente}/`, valida que `len(in_df) == number_of_groups` | Carga masiva del CSV devuelto por el equipo externo, mismo chequeo de conteo |
| 3. Cálculo de costo unitario | `unt_sfc = txn_sfc/txn_cnt`, `unt_est_sch_fee_amt = est_sch_fee_amt/txn_cnt` | Igual — el legacy también divide para obtener el costo por transacción |
| 4. Update del reporte | Join por `app_id` (clave única del reporte agregado, 1:1) → `coalesce(nuevo, viejo)` en las 4 columnas de costo → sobreescribe `final/{mes}/report/` | `UPDATE` del registro de reporte por su PK |
| 5. Propagación al detalle | Join `detail_df` ↔ `cost_by_group` por las 20 `GROUP_DIMS` (`eqNullSafe`, no por llave de transacción — es intencional: todas las transacciones del mismo grupo reciben el mismo costo total replicado, igual que legacy, no dividido) → `final/{mes}/detail/` | `get_update_detail()` — mismo propósito, mismo criterio de "replicar el total del grupo en cada fila" |
| 6. Cierre | Actualiza `summary.json` (`updated_at`, `number_of_updated_rows`) | No hay equivalente explícito de "summary" |

**Mecánica exacta de la etapa 5 (`update_report_and_propagate()`):** NO es un recálculo — es una copia completa de `state/detail/` (`d.*`, todas las columnas originales intactas, ninguna fila se pierde/colapsa) más un join que AGREGA las 4 columnas de costo (`transaction_scheme_fee_cost`, `unitary_scheme_fee_cost`, `estimated_scheme_fee_cost`, `unitary_estimated_scheme_fee_cost`) usando las 20 `GROUP_DIMS` como llave. Por eso `state/detail` tiene 34 columnas y `final/detail` tiene 38 (34 + las 4 de costo) — nunca se recalculan ni reordenan las 34 originales.

## Llave de transacción para cruzar con `get_transaction.py` (relevante para el TODO `scheme_fees_amount`)

`get_transaction.py` identifica cada transacción con `file_id` (= `content_hash`) + `row_id`:
- Visa: `row_id` = columna `record` (campo estable del operational)
- Mastercard: `row_id` = columna `ref_id` (campo estable, el mismo que usa `lmbd-mc-store` para mergear CLN+CAL+ITX)

En `scheme_fee.py`, el equivalente es `app_hash_file` (=`content_hash`) + `app_id` (detalle, renombrado de `source_row_id`, **no confundir con el `app_id` del reporte agregado — mismo nombre, conceptos distintos, no colisionan porque nunca conviven en el mismo DataFrame**):
- Visa BASEII/SMS: `app_id` = columna `record` → **coincide exacto** con `get_transaction.py`, cruce directo posible.
- Mastercard: **corregido 2026-07-08** — antes usaba `F.monotonically_increasing_id()` (mismo antipatrón ya conocido en el proyecto, ver gotcha de `glue-mc-interchange` en `gotchas.md`), no correspondía a nada reproducible. Ahora usa `F.col("ref_id")`, igual que `get_transaction.py` — el cruce por `app_hash_file`+`app_id` ya es válido para ambas marcas. **Pendiente de validar con datos reales** (fix aplicado, aún no corrido contra producción).

## Qué contiene cada carpeta de `s3-analytics/{cliente}/scheme_fee/{report_month}/`

| Carpeta | Contenido | Cuándo se escribe |
|---|---|---|
| `state/detail/` | Detalle transaccional completo (una fila por transacción, ~34 columnas: `app_type_file`, `app_customer_code`, `app_hash_file`, `app_id`, `table_description`, `app_processing_date`, `account_number`, `card_acceptor_id`, dimensiones de negocio, `exchange_rate`/`report_amount`, etc.) — **sin costo todavía** | `--mode generate` (o de nuevo si `--force true`) |
| `state/report/` | Reporte agregado por las 20 `GROUP_DIMS` (una fila por grupo único, ~28 columnas), con `app_id` ya asignado (mismo que en el CSV exportado) — costo en `NULL` porque aún no volvió el CSV con las cuotas reales | `--mode generate` |
| `summary.json` | Metadata de la ejecución: `execution_id`, `number_of_groups`, `number_of_inserted_rows`, ruta del CSV OUT, timestamps | Creado en `--mode generate`, actualizado (`updated_at`/`number_of_updated_rows`) en `--mode read` |
| `final/report/` | Copia del reporte agregado (28 columnas) con las 4 columnas de costo ya completadas desde el CSV de vuelta | `--mode read` |
| `final/detail/` | Copia del detalle transaccional (38 columnas = 34 originales + 4 de costo), con el costo del grupo ya propagado a cada fila individual — fuente futura para resolver `scheme_fees_amount` en `get_transaction.py` (usar la columna **unitaria**, no el total, para no sobrecontar) | `--mode read` |

`state/` = "antes de mandar al equipo externo" (sin costo). `final/` = "después de que vuelve con el costo calculado". Separados para no pisar el detalle original si `--mode read` necesita reintentarse.

Muestras locales de ambos (para revisión manual de estructura/campos) en `tst_files/scheme_fee_parquet_samples/{generate_state,read_final}/` — regeneradas 2026-07-08 contra la corrida ya validada (`execution_id=202678_1783544266`). Los CSV de ida (`OUT_SBSA_202601.csv`) y vuelta simulado (`IN_SBSA_202601_simulated.csv`) también están en `tst_files/scheme_fee_reports/read_test/`, misma corrida — versiones anteriores (tanto locales como en `s3-scheme-fee/{IN,OUT}/SBSA/`) fueron eliminadas, solo se conserva la última.

## Estado de validación (actualizar in-place, no acumular)

**2026-07-08 — `--mode generate` y `--mode read` VALIDADOS END-TO-END con los 2 fixes siguientes, aplicados a `scheme_fee.py`:**
1. NaN→None en columnas numéricas de `report_pdf` antes de `spark.createDataFrame()` en `run_generate()` (bug: `range_program_id` quedaba NaN en vez de NULL en `state/report/`, rompiendo el join `eqNullSafe` de `--mode read` para esos grupos).
2. `app_id` de Mastercard en `transform_mastercard_scheme_fee()`: `F.col("ref_id")` en vez de `F.monotonically_increasing_id()` (alineado con `get_transaction.py`, necesario para poder cruzar el costo de scheme fee de vuelta a una transacción MC específica).

**Validación real ejecutada:** `--mode generate --force true` (SBSA/202601, 10 workers G.1X temporal solo para esa corrida, revertido a 2 al terminar) → `--mode read` con CSV simulado rehecho contra el nuevo `app_id`. Resultado:
- 0 filas de detalle con `transaction_scheme_fee_cost` NULL en `final/detail/` (antes del fix: 40,302 filas / 755 grupos con `range_program_id` NaN).
- `range_program_id` en `state/report/`: 755 nulls reales, 0 NaN (antes: 0 nulls reales, 755 NaN) — confirma el fix.
- `app_id` de Mastercard cruzado 5/5 contra el operational real por `content_hash`+`ref_id` (mismo PAN en ambos lados) — confirma que ya no es un ID sintético de Spark.

**Pendiente:** commitear ambos fixes a git (decisión del usuario, aplicados solo en S3 por ahora). Script en S3: `s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/scheme_fee.py`.
