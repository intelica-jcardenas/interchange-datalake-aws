# Decisiones de arquitectura

Decisiones no obvias tomadas durante el desarrollo. Cada entrada explica el **qué**, el **por qué** y las **alternativas descartadas**.

Las decisiones con implementación/validación extensas fueron resumidas aquí — el detalle completo (pasos de implementación, tablas de validación, reprocesos) está en `.claude/memory/decisions_archive.md` (no cargado automáticamente).

---

## `lmbd-rules-refresh` (automatización de visa_rules/mc_rules) — probado end-to-end sobre `lmbd-test-1`, con trigger S3 real activo — 2026-08-10

**Contexto:** `lambdas/rules-refresh/` (código en el repo, ver su `README.md`) reemplaza el proceso manual de refrescar `visa_rules/data.parquet`/`mc_rules/data.parquet` en `s3-reference` — hasta ahora se hacía corriendo `tst_files/interchange_rules/build_and_compare_rules.py` a mano cada vez que el área de Data Quality entregaba un excel nuevo. El Lambda valida el excel, calcula el diff contra el parquet actual, respalda el anterior a `{visa_rules|mc_rules}/history/`, publica el nuevo y archiva el excel de origen.

**Probado con infraestructura prestada** (`itl-0004-itx-dev-intchg-02-lmbd-test-1`, Lambda huérfano de pruebas ya existente, rol `lmbd-vi-role` con permisos completos sobre `s3-reference`) — no se creó todavía un Lambda/rol propio.

**Sin tabla de auditoría en DynamoDB — decisión explícita del usuario (2026-08-10), no un pendiente.** Se había redactado un schema propuesto (`dynamodb/schemas/rules_control.json`) y el handler tenía un flag `RULES_CONTROL_ENABLED` para tolerar operar sin la tabla creada — todo eso se **eliminó del código** (no solo se deshabilitó): es un proceso chico y de baja frecuencia (el negocio sube un excel cada tanto, no un flujo transaccional de alto volumen como `file_control`), no justifica una tabla propia. El resultado de cada refresh (diff, éxito/fallo, motivo de rechazo) queda en CloudWatch — suficiente para este volumen. Si en el futuro se necesita auditoría estructurada, evaluarlo de nuevo en ese momento — no hay ningún rastro de la tabla en el código, README, ni schemas del repo.

**Trigger S3→Lambda configurado y validado con una subida real** (`aws lambda add-permission` + `aws s3api put-bucket-notification-configuration`, prefijo `interchange_rules/`, sufijo `.xlsx`/`.xls`): subir un excel a `interchange_rules/{VISA,MASTERCARD}/` en `s3-reference` ahora dispara el refresh solo — sin invocación manual. Diseño ya acordado con el usuario (ver README del Lambda): auto-publica si la validación estructural pasa, sin aprobación manual intermedia.

**Bug real encontrado y corregido en la misma sesión (2026-08-10):** un excel que no se puede leer/parsear (ej. la marca equivocada subida a la carpeta equivocada — `sheet_name` no coincide) lanzaba un `ValueError` crudo que NO era capturado por el único `except RulesValidationError` del Lambda — escapaba al manejador genérico, que lo relanza a propósito para que Lambda reintente (pensado para errores de infraestructura, no de contenido). Consecuencia: reintentos automáticos inútiles (~60s de espera, confirmado en CloudWatch) y el archivo quedaba atascado en la carpeta de entrada, sin moverse a `_archive/` ni `_rejected/` (sin DLQ configurado, tras agotar los reintentos el evento se descarta en silencio). **Fix:** el `pd.read_excel(...)` ahora está envuelto en su propio `try/except`, relanzando como `RulesValidationError` — mismo camino que cualquier otra validación de contenido fallida (rechazo limpio a `_rejected/`, sin reintento). Validado re-invocando contra el mismo archivo atascado → `FAILED` limpio.

**Hallazgo de contenido real durante la primera prueba (no bug del Lambda):** el excel `VISA Reglas Intercambio V37.xlsx` que estaba en el repo (`tst_files/interchange_rules/`, gitignored) tenía contenido distinto al que generó el `visa_rules/data.parquet` publicado el 2026-07-28 — 3 filas (`intelica_id` 184/185/188, reglas "VE OCT CR"/"VE OCT COML", vigentes desde 2026-06-01 sin cierre) con un código nuevo (`"LA"`) agregado a `business_application_id`, condición activa del motor de reglas (`interchange.py:297`). Confirmado con el usuario: el área de Data Quality actualiza este excel para legacy sin renombrar la versión — cambio real, se dejó publicado.

**Riesgo teórico planteado y DESCARTADO con pruebas reales (no solo lectura de código):** se sospechó que la nueva carpeta `mc_rules/history/` (creada por primera vez por este Lambda) podía romper o contaminar en silencio `glue-mc-interchange`, porque ese job lee el PREFIJO `mc_rules/` completo (`spark.read.parquet`), a diferencia de Visa que lee el archivo `visa_rules/data.parquet` exacto. Probado con un `history/` sintético + un job real (EBGR, `1A243466B3AC24A91E3B5376494943B3`, 2026-01-05, MTI 1240): no rompió, y un log temporal de conteo (`df_rules.count()`) confirmó que Spark (Glue 4.0) no recorre esa subcarpeta (`24,531` filas, ni una más — no dobló el conteo). **El riesgo no era real en este entorno.** Se aplicó igual el fix (`rules_path` → `mc_rules/data.parquet` exacto, mismo patrón que Visa) por robustez/claridad, no por un bug confirmado — validado con un 3er run dando resultado idéntico al baseline (`rules_needed=1557`, `processed=1 failed=0`, cero regresión). Cambio en `glue/scripts/mastercard/interchange/interchange.py` línea ~1635, ya subido a S3 (`aws s3 cp` directo al `ScriptLocation` real — este SÍ es un job productivo en uso, a diferencia del Lambda de prueba).

**Pendiente antes de confiar en esto para producción real:** layer con `openpyxl` y rol IAM dedicados (sigue sobre infra prestada de `lmbd-vi-role`), decidir si se migra de `lmbd-test-1` a un Lambda propio antes de dejar el trigger activo de forma permanente. Ver `.claude/memory/pending.md`.

---

## Auditoría completa de lecturas a `s3-reference` (prefijo/directorio vs archivo exacto) — 2026-08-10, 2 fixes aplicados y validados

**Motivo:** a raíz del fix de `mc_rules` (ver decisión de arriba), el usuario pidió auditar TODAS las lecturas de tablas de referencia en los 8 scripts que tocan `s3-reference` (`visa/calculate.py`, `visa/interchange.py`, `mastercard/calculate.py`, `mastercard/interchange.py`, `get_transaction.py`, `scheme_fee.py`, `vi_data_quality.py`, `mc_data_quality.py`) — recordaba un incidente real anterior con un CSV en `currency/` que rompió un proceso que esperaba solo `.parquet`.

**Hallazgo confirmado:** `currency/` en `s3-reference` SÍ tiene un archivo no-parquet real hoy — `currency/m_currency.csv/m_currency.csv` (subido 2026-08-05). No es basura: es el input real de la tabla de catálogo Glue `m_currency_csv` (`itl_0004_itx_dev_02_glue_database_exchange_rates`), consumida por `glue-exchange-rates`/`format_exchange_rates.py` vía `glueContext.create_dynamic_frame.from_catalog(...)` — **no se puede borrar** sin romper ese job. Coincide con el recuerdo del usuario (mismo tipo de problema, misma carpeta) — posiblemente el mismo archivo resubido, o el mismo patrón repitiéndose.

**3 lecturas en modo PREFIJO/DIRECTORIO encontradas (mismo patrón de riesgo que `mc_rules`), 2 corregidas:**
1. `currency/` en `glue-mc-interchange` (`interchange.py:1634`, ahora `:1636`) y `mc_data_quality.py` (×2 ocurrencias) → cambiado a `currency/data.parquet` exacto.
2. `mastercard_business_transaction_type/` en `mc_data_quality.py` (×2) → `mastercard_business_transaction_type/data.parquet` exacto.
3. `validation_conditions/` en `mc_data_quality.py` (×2, inconsistente con `vi_data_quality.py` que ya leía el archivo exacto) → `validation_conditions/data.parquet` exacto.

**Confirmado que NO rompía hoy** (evidencia real, no solo teoría): `glue-mc-interchange` corrió 3 veces el mismo día con el CSV ya presente desde el 2026-08-05, todas `SUCCEEDED` — mismo comportamiento de Spark que con `mc_rules/history/` (no recorre subcarpetas de un nivel que no siguen el patrón Hive `clave=valor`). El fix es igualmente defensivo/de robustez, no la corrección de un bug activo confirmado.

**Resto de lecturas de referencia en los 8 scripts: ya usaban archivo exacto** (`country`, `region`, `mastercard_iar`, `mastercard_brand_product`, `visa_rules`, `visa_ardef`, `visa_bin_products`, `mastercard_bin_products`, `local_switch`, `scheme_fee_bin_products`, `bin_funding_source`, `visa_business_transaction_type`, `size_ticket`, `visa_business_transaction_cycle`, y `validation_conditions` en `vi_data_quality.py`). `exchange-rates-glue/brand=X/exchange_date=Y/` es la única lectura de directorio que queda — partición Hive real por diseño (múltiples part-files legítimos por partición), contenido confirmado limpio.

**Lambdas (`lmbd-mc-clean`, `lmbd-mc-iar`, `lmbd-vi-ardef`) confirmadas sin este riesgo, por diseño:** todas usan `S3.get_object(Bucket=..., Key="carpeta/data.parquet")` (key exacta) + `pd.read_parquet(io.BytesIO(body))` — `get_object` no puede "leer una carpeta entera", nunca hace listado de directorio. Sin cambios necesarios ahí.

**Desplegado y validado (2026-08-10):** ambos scripts subidos a S3 real (`ScriptLocation`). `glue-mc-interchange` — smoke test contra EBGR `1A243466B3AC24A91E3B5376494943B3`/2026-01-05/MTI 1240: `SUCCEEDED`, `rules_needed=1557` (idéntico al baseline pre-fix), sin regresión. `mc_data_quality.py` — smoke test EBGR/2026-01-05/`I`/`default`: `SUCCEEDED` — **primera ejecución de este job contra datos reales en toda su historia** (antes solo se había probado sin datos reales, ver `gotchas.md`), las 5 lecturas de referencia (`mastercard_business_transaction_type`, `currency`, `validation_conditions` ×2 pasadas) confirmadas leyendo el archivo exacto, output generado en `s3-analytics/EBGR/reports/quality/range_2026-01-05_2026-01-05/`.

**Sin commitear.**

---

## Scheme Fee (`glue-scheme-fee`) — mapeo de etapas `generate`/`read` vs legacy y estructura de `s3-analytics/` — VALIDADO 2026-07-08, referencia permanente (fusionado desde `scheme_fee_generate_read_pipeline.md`, retirado 2026-07-31 por ya no aplicar como "temporal" — sin urgencia de cierre, EBGR no usa cuotas)

**Contexto:** `glue-scheme-fee` replica el módulo legacy de cuotas (`managment.py`/`getquery.py`, proceso EC2) en 2 modos: `--mode generate` (arma el detalle+reporte y exporta un CSV para el equipo externo que calcula las cuotas) y `--mode read` (lee el CSV de vuelta con los costos y los propaga). El histórico de bugs/investigaciones/decisiones de diseño más extenso vive en la memoria de usuario `scheme_fee_job_design.md` (no versionada en este repo) — esta entrada es solo el mapeo de etapas + estructura de carpetas.

**Etapas de `--mode generate` vs legacy:**

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

**Etapas de `--mode read` vs legacy:**

| Etapa | `scheme_fee.py` (nuevo) | Legacy |
|---|---|---|
| 1. Validación previa | Requiere `summary.json` existente (si no, error "correr generate primero") | Requiere que el INSERT de detalle/reporte ya se haya hecho |
| 2. Lectura del CSV de vuelta | `pd.read_csv()` del archivo en `s3-scheme-fee/IN/{cliente}/`, valida que `len(in_df) == number_of_groups` | Carga masiva del CSV devuelto por el equipo externo, mismo chequeo de conteo |
| 3. Cálculo de costo unitario | `unt_sfc = txn_sfc/txn_cnt`, `unt_est_sch_fee_amt = est_sch_fee_amt/txn_cnt` | Igual — el legacy también divide para obtener el costo por transacción |
| 4. Update del reporte | Join por `app_id` (clave única del reporte agregado, 1:1) → `coalesce(nuevo, viejo)` en las 4 columnas de costo → sobreescribe `final/{mes}/report/` | `UPDATE` del registro de reporte por su PK |
| 5. Propagación al detalle | Join `detail_df` ↔ `cost_by_group` por las 20 `GROUP_DIMS` (`eqNullSafe`, no por llave de transacción — es intencional: todas las transacciones del mismo grupo reciben el mismo costo total replicado, igual que legacy, no dividido) → `final/{mes}/detail/` | `get_update_detail()` — mismo propósito, mismo criterio de "replicar el total del grupo en cada fila" |
| 6. Cierre | Actualiza `summary.json` (`updated_at`, `number_of_updated_rows`) | No hay equivalente explícito de "summary" |

**Mecánica exacta de la etapa 5 (`update_report_and_propagate()`):** NO es un recálculo — es una copia completa de `state/detail/` (`d.*`, todas las columnas originales intactas, ninguna fila se pierde/colapsa) más un join que AGREGA las 4 columnas de costo (`transaction_scheme_fee_cost`, `unitary_scheme_fee_cost`, `estimated_scheme_fee_cost`, `unitary_estimated_scheme_fee_cost`) usando las 20 `GROUP_DIMS` como llave. Por eso `state/detail` tiene 34 columnas y `final/detail` tiene 38 (34 + las 4 de costo) — nunca se recalculan ni reordenan las 34 originales.

**Llave de transacción para cruzar con `get_transaction.py`:** `get_transaction.py` identifica cada transacción con `file_id` (= `content_hash`) + `row_id` (Visa: columna `record`; Mastercard: columna `ref_id`, la misma que usa `lmbd-mc-store` para mergear CLN+CAL+ITX). En `scheme_fee.py`, el equivalente es `app_hash_file` (=`content_hash`) + `app_id` (detalle, renombrado de `source_row_id` — no confundir con el `app_id` del reporte agregado, mismo nombre, conceptos distintos, nunca conviven en el mismo DataFrame): Visa BASEII/SMS usa `app_id` = columna `record` (coincide exacto, cruce directo posible); Mastercard usa `F.col("ref_id")` (fix 2026-07-08, ver gotcha abajo) — el cruce por `app_hash_file`+`app_id` es válido para ambas marcas.

**Desambiguación del duplicado on-us (2026-07-10, ajustado a literales exactos del legacy tras revisar `getquery.py`):** `file_id`+`row_id`/`app_id` por sí solos son idénticos entre la fila original y su copia on-us (`_apply_duplicate_on_us()` solo cambia `business_mode_code`/`business_mode_id`). La llave completa para el join es `file_id` + `row_id` + `business_mode_code` (traducido A/I ↔ ACQUIRING/ISSUING en el momento del join). `table_description` replica exacto los literales de `get_on_us_visa`/`get_sms_on_us_visa`/`get_on_us_mastercard` (legacy): duplicado VISA BASEII = `"VISA ON-US DUP (ACQ TO ISS)"`, SMS = `"VISA ON-US DUP (SMS TO ISS)"`, MC = `"MASTERCARD ON-US DUP (ACQ TO ISS)"`; filas normales de VISA BASEII distinguen `"VISA ACQ"`/`"VISA ISS"` y MasterCard `"MASTERCARD ISS AND ACQ"`, replicando exacto `get_acquirer_visa`/`get_issuers_visa`/`get_transactions_mastercard`. Sirve como marcador explícito adicional al join — no depende únicamente de `business_mode_id`, campo con historial de inconsistencia de mayúscula/minúscula entre Visa y Mastercard (ver `gotchas.md`).

**Qué contiene cada carpeta de `s3-analytics/{cliente}/scheme_fee/{report_month}/`:**

| Carpeta | Contenido | Cuándo se escribe |
|---|---|---|
| `state/detail/` | Detalle transaccional completo (una fila por transacción, ~34 columnas: `app_type_file`, `app_customer_code`, `app_hash_file`, `app_id`, `table_description`, `app_processing_date`, `account_number`, `card_acceptor_id`, dimensiones de negocio, `exchange_rate`/`report_amount`, etc.) — **sin costo todavía** | `--mode generate` (o de nuevo si `--force true`) |
| `state/report/` | Reporte agregado por las 20 `GROUP_DIMS` (una fila por grupo único, ~28 columnas), con `app_id` ya asignado (mismo que en el CSV exportado) — costo en `NULL` porque aún no volvió el CSV con las cuotas reales | `--mode generate` |
| `summary.json` | Metadata de la ejecución: `execution_id`, `number_of_groups`, `number_of_inserted_rows`, ruta del CSV OUT, timestamps | Creado en `--mode generate`, actualizado (`updated_at`/`number_of_updated_rows`) en `--mode read` |
| `final/report/` | Copia del reporte agregado (28 columnas) con las 4 columnas de costo ya completadas desde el CSV de vuelta | `--mode read` |
| `final/detail/` | Copia del detalle transaccional (38 columnas = 34 originales + 4 de costo), con el costo del grupo ya propagado a cada fila individual — fuente futura para resolver `scheme_fees_amount` en `get_transaction.py` (usar la columna **unitaria**, no el total, para no sobrecontar) | `--mode read` |

`state/` = "antes de mandar al equipo externo" (sin costo). `final/` = "después de que vuelve con el costo calculado". Separados para no pisar el detalle original si `--mode read` necesita reintentarse.

**Validación (2026-07-08):** `--mode generate --force true` (SBSA/202601) → `--mode read` con CSV simulado. 0 filas de detalle con `transaction_scheme_fee_cost` NULL en `final/detail/` (antes del fix de abajo: 40,302 filas/755 grupos NULL). `app_id` de Mastercard cruzado 5/5 contra el operational real por `content_hash`+`ref_id`.

**Estado (2026-07-31):** validado end-to-end con costos DUMMY (no hay CSV real del equipo externo todavía — ver `pending.md`). EBGR (cliente prioritario actual) no usa cuotas, así que esto queda sin urgencia de cierre; SBSA sigue sin definición de otras áreas sobre si las necesita.

---

## Estandarización de configuración de Glue Jobs (2026-07-30) — encontrada contaminación de consola, corregida en 7 de 9 jobs

**Contexto:** durante el reproceso masivo de MC (token_flag), `glue-mc-calculate` corría archivo por archivo en vez de en paralelo. Investigando eso se encontró configuración desalineada en varios de los 9 Glue jobs del proyecto — no touchada por ningún script nuestro (confirmado revisando qué llamadas se hicieron en la sesión), aparentemente por ediciones manuales vía la consola de AWS Glue Studio en distintos momentos.

**Hallazgo 1 — `glue-mc-calculate` tenía infraestructura muy distinta a sus 3 pares** (`vi-calculate`, `vi-interchange`, `mc-interchange`): `MaxConcurrentRuns=1` (debía ser 50, documentado desde junio), `NumberOfWorkers=10` (vs 2 documentado en `CLAUDE.md`), `MaxRetries=0` (vs 1), `Timeout=2880` min/48h (vs 20). **Corregido:** alineado 100% a `mc-interchange` (`G.1X×2, MaxRetries=1, Timeout=20, MaxConcurrentRuns=50`).

**Hallazgo 2 — `glue-mc-interchange` y `glue-mc-data-quality` tenían `DefaultArguments` contaminados con la misma firma exacta**: parámetros de una ejecución puntual grabados como default permanente (`client_id=SBSA`/`process_date=2026-02-18`/`file_type=IN` en interchange; `CUSTOMER_CODE=EBGR`/`START_DATE`/`END_DATE`/etc. en data-quality), más `s3a://` en vez de `s3://`, y flags de monitoreo (`enable-spark-ui`, `spark-event-logs-path`, `enable-continuous-log-filter`, `spark.eventLog.rolling.enabled`) ausentes en el resto de los jobs — patrón típico de guardar un job desde la consola de Glue Studio después de una prueba manual. **Corregido en ambos** — solo quedan los argumentos de infraestructura (buckets, tablas DynamoDB, `TempDir`, `conf`, logging estándar).

**Hallazgo 3 — `glue-get-transaction` y `glue-scheme-fee` también tenían parámetros de negocio fijos**: `client_code`/`start_date`/`end_date`/`report_suffix`/`scheme_fee` en get-transaction; `client_code`/`report_month`/`mode`/`force`/`in_file_key` en scheme-fee. Por diseño ambos jobs son "un cliente/una decisión por ejecución" (ver decisión "Por qué el reporting job ejecuta un cliente por vez" y el diseño `--mode generate`/`--mode read` de scheme-fee) — nunca deberían tener esto como default. **Corregido en ambos.**

**Estandarización final de los 4 jobs principales** (`vi-calculate`, `vi-interchange`, `mc-calculate`, `mc-interchange`): mismo `Role`, `MaxRetries=1`, `Timeout=20`, `MaxConcurrentRuns=50`, mismos flags base (`enable-metrics`, `enable-job-insights`, `enable-continuous-cloudwatch-log`, `job-bookmark-option: job-bookmark-disable`, `conf`, `job-language`) — única diferencia intencional confirmada: `vi-interchange` en `G.2X × 4` (vs `G.1X × 2` del resto).

**Pendiente de confirmar (no tocado):** `vi-data-quality`/`mc-data-quality` usan `Role: itl-0004-itx-dev-intchg-02-glue-test-role` en vez de `...-glue-role` — probablemente intencional dado que ninguno está integrado a un Step Function todavía, sin confirmar con el usuario. `vi-data-quality` tiene `NumberOfWorkers=10`/`MaxConcurrentRuns=50` (vs 2/1 de sus pares de reportería) — tampoco confirmado si es deliberado. `exchange-rates` tiene `Timeout=30` (vs 2880 del grupo) y `job-bookmark-option: job-bookmark-enable` — evaluado y descartado como problema, calza con su diseño de orquestador/worker en chunks.

**Confirmado con sync completo 2026-08-03:** `vi-data-quality.MaxConcurrentRuns` cambió a `1` en algún momento entre esta auditoría y esa fecha (visto en el `config.json` local antes de resincronizar), y volvió a `50` en AWS con `LastModifiedOn=2026-07-31T18:37:35-05:00` — cambio hecho fuera de esta sesión, no por nosotros. Confirmado con `sync-glue.ps1 -Resource jobs` (los 9 jobs completos): los otros 8 quedaron sin ningún diff (script+config+args idénticos a AWS) — el único drift real fue este. Repo local actualizado para reflejar el valor actual (`50`). `NumberOfWorkers=10` sigue sin cambios. Sigue sin confirmarse si el valor correcto/intencional es `1` o `50`.

**Regla nueva establecida con el usuario:** los scripts de reproceso/automatización NUNCA deben llamar `aws glue update-job` para pasar parámetros de negocio — siempre `Arguments` en `start_job_run` (por-ejecución, no persiste). Cambios de configuración persistente solo a pedido explícito del usuario, mismo nivel de cuidado que los commits de git. Ver memoria de usuario `feedback_no_glue_config_changes_in_scripts.md`.

**Todos los `config.json`/`args.json` locales de los 7 jobs tocados fueron re-sincronizados** desde AWS (`sync-glue.ps1`) — los scripts `.py` en sí no cambiaron, solo la configuración.

---

## Por qué token_flag en glue-mc-interchange se re-deriva de token_requestor_id_pds_59 (no de electronic_commerce_indicator_2_pds_52_2) — DESPLEGADO 2026-07-30, sin validación de match decisivo

**Decisión:** `token_flag` (condición de `mc_rules` usada por `_simple_rule_condition("token_flag", "token_flag")` en `glue-mc-interchange`) ya no se toma directo del campo `electronic_commerce_indicator_2_pds_52_2` — se deriva de `token_requestor_id_pds_59` con la clasificación `"1"` si empieza con `501`, `"0"` si tiene valor pero no empieza así, `"BLANK"` si es null. Además, `mastercard_fields` (DynamoDB) tenía `token_requestor_id_pds_59.type_mti="1644, 1740"` — se ensanchó a `"1240, 1442, 1644, 1740"`.

**Por qué:** revisando qué campos nuevos traía el excel `MASTERCARD Reglas Intercambio V23.xlsx` (ver `.claude/memory/pending.md`), se encontró que `token_flag` (columna ya existente en `mc_rules`, no nueva) se calculaba con un campo semánticamente equivocado (ECI, no Token Requestor ID). La causa raíz completa solo se pudo confirmar consultando `control.t_mastercard_adapter` en PRD (Postgres) — un catálogo de campos IPM real de legacy, análogo a nuestra tabla `mastercard_fields`, que no se conocía hasta esta investigación. Ahí `token requestor id` = PDS 59, sin restricción de MTI (`message_type_identifier IS NULL`). Se confirmó con datos reales (`operational.dh_mastercard_data_element_sbsa_out_20260727`): 856 filas de MTI 1240 con `token_requestor_id=501` — el campo SÍ existe y se puebla para 1240, algo que nuestro propio catálogo no permitía capturar por el `type_mti` mal scopeado.

**Por qué el fix es solo de datos (DynamoDB) + un rename en interchange.py, no un cambio de interpreter/transform:** `lmbd-mc-transform`/`lmbd-mc-extract` ya filtran qué PDS expandir/renombrar dinámicamente leyendo `mastercard_fields` y comparando `type_mti` contra el MTI en curso (`lambdas/mastercard/transform/src/handler.py` línea ~263-268, `extract/src/handler.py` línea ~278) — ensanchar el scope en DynamoDB alcanza para que ambas Lambdas empiecen a producir la columna para 1240/1442, sin tocar su código.

**Costo aceptado — reproceso necesario:** el campo crudo nuevo solo existe en archivos que se vuelvan a pasar por `lmbd-mc-transform` en adelante. Los archivos MC 1240/1442 ya procesados no tienen `token_requestor_id_pds_59` en su CLN hasta que se reprocesen desde transform (no hace falta re-correr el interpreter — el RAW ya trae el TLV completo, `type_mti` solo afecta qué se expande a columna).

**Validación inicial (2026-07-30):** cadena completa `transform→extract→clean→calculate→interchange` corrida contra 2 archivos reales (SBSA `E0C717BF7FC307E63E8E29918E813B02`/2026-01-03, EBGR `1A243466B3AC24A91E3B5376494943B3`/2026-01-05) — 0 errores en las 10 corridas. Dato crudo confirmado en cada etapa (`PDS_59` en TRA → `token_requestor_id_pds_59` en EXT/CLN, valores reales `501xxxxxxxx`). La lógica de clasificación está verificada por revisión de código 1:1 contra `adapters.py` (`left(token_requestor_id::text,3)='501'`).

**Alternativa descartada:** mantener `electronic_commerce_indicator_2_pds_52_2` y ajustar solo la comparación — descartada de inmediato, es un campo distinto (ECI, no Token Requestor ID), no hay forma de que produzca la clasificación correcta sin importar cómo se compare.

**Fix relacionado, mismo día:** auditoría completa de las 32 columnas de `mc_rules` contra `mastercard_interchange_rule_assign` (`adapters.py` línea ~5500, la función real de matching de MC — tampoco se conocía) encontró un solo gap adicional: `issuer_bin_8` se calculaba en `interchange.py` pero nunca se agregaba a `simple_conditions`. Agregado (`_simple_rule_condition("issuer_bin_8", "issuer_bin_8")`) — impacto real hoy: cero (0 reglas activas lo usan en `mc_rules_new.parquet`), deja el motor completo para cuando Mastercard lo empiece a poblar. El resto de columnas de `mc_rules` (`fee_category`, `fee_tier`, `masterpass_incentive_indicator`, `additional_data`) se confirmó que ya están correctamente excluidas como no-condición, coincidiendo con la propia lista de exclusión de legacy.

**Reproceso masivo SBSA enero 2026 completado (2026-07-30):** `transform→extract→clean` (104/104, con verificación real de schema, no solo status) → `glue-mc-calculate` (104/104) → `glue-mc-interchange` (104/104) → `lmbd-mc-store` (104/104 reales — 2 reportaron `Read timeout` del cliente boto3 a ~910s pero CloudWatch confirma que ambos Lambdas terminaron OK, mismo falso negativo ya documentado en `gotchas.md`). `mc_rules_new.parquet` (excel V23) ya subido a S3 el mismo día (ver `pending.md`).

**Validación final del match decisivo (2026-07-30, Athena sobre el mes completo reprocesado, no solo 2 archivos sueltos):** `token_requestor_id_pds_59`/`token_flag` confirmados en el catálogo de Glue re-crawleado — volumen real SBSA enero 2026 (IPM_1240+1442, ambas direcciones): `token_flag='1'`=6,771,306, `='0'`=1,119, `='BLANK'`=72,854,147. Cruce completo de las 83 combinaciones reales (`intelica_id`, `region_country_code`, `ird`) que reciben transacciones `token_flag='1'` contra las 1,031 reglas activas en enero 2026 con condición `token_flag` real: **0 matches decisivos y 0 candidatos de intercepción** (ninguna de las 83 combinaciones observadas comparte siquiera el `(region_country_code, ird)` con alguna regla `token_flag`-condicionada). Causa estructural, no un bug: las reglas `token_flag` activas se concentran en `region_country_code` de países caribeños específicos (`AW,BB,BS,CW,DO,KY,SX,TC,TT`) más `9` (interregional) con `ird∈{75,83,95,ES,PS}` — el tráfico real de SBSA con `token_flag=1` cae en `ird∈{61,63,CB,EB,EG,EV,EW,IP,Q1,Q2,Q3,YA..YI}`, conjunto totalmente disjunto. El mix de producto/país de SBSA simplemente no alcanza los segmentos que Mastercard tarifica distinto por token — no hay forma de generar un match decisivo con el tráfico real de este cliente en este período, más allá de cuánto se reprocese. La corrección en sí (derivar de `token_requestor_id_pds_59`, no de ECI) sigue confirmada por revisión de código 1:1 contra `adapters.py` y por el volumen real y significativo del campo (6.77M filas con `token_flag=1` correctamente identificadas, antes calculadas con un campo semánticamente equivocado).

**Reproceso masivo EBGR enero 2026 completado (2026-07-30) — primer cliente oficial, priorizado sobre SBSA:** el usuario indicó que EBGR es el primer cliente que pasa oficialmente al nuevo sistema (SBSA queda pendiente de definición con otras áreas por el tema de cuotas), así que se repitió el mismo reproceso ahí. 208 archivos EBGR/MC en `file_control-02` para enero 2026 (124 IN + 84 OUT). Paso 1 (`transform→extract→clean`): **143/208 verificados** (142 + 1 retry por throttling) — los 65 restantes (100% `file_type=OUT`, parejo en las 31 fechas) tienen `store_result=null` en DynamoDB, confirmado con el 100% de la muestra: nunca completaron el pipeline original, no son candidatos válidos (no hay bug, nada que reprocesar ahí). Confirmado que los 124 archivos IN (el universo completo que legacy también ve) quedaron 100% dentro del set verificado. Paso 2 (`glue-mc-calculate`, 143 archivos): 143/143 SUCCEEDED. Paso 3 (`glue-mc-interchange`, 143 archivos): 143/143 VERIFICADOS OK (CloudWatch `[PAIR]`). Paso 4 (`lmbd-mc-store`, 143 archivos): 143/143 SUCCESS real, sin falsos negativos de timeout esta vez (archivos EBGR mucho más chicos que SBSA, mayoría <1s).

**Verificación final EBGR (2026-07-30, Athena sobre el mes completo, catálogo re-crawleado):** `token_requestor_id_pds_59` confirmado en el Parquet físico reprocesado. Volumen real EBGR enero 2026 (IPM_1240+1442): `token_flag='1'`=16,751,565 (51.8% del total — proporción mucho mayor que SBSA, donde era 8.5%), `='0'`=4,455, `='BLANK'`=15,581,980. Mismo cruce contra las 1,031 reglas activas con condición `token_flag` real: **0 matches decisivos y 0 candidatos de intercepción** en las 117 combinaciones `(intelica_id, region_country_code, ird)` reales con match de regla — mismo motivo estructural que SBSA: EBGR opera mayormente en `region_country_code='5'` (Europa) y `'9'` (interregional) con `ird` que no se solapan con los `ird∈{75,83,95,ES,PS}` (región 9) ni con los países caribeños que sí llevan condición `token_flag` activa.

**Hallazgo adicional, no relacionado al fix de hoy — anotado, sin investigar:** de las ~16.75M transacciones `token_flag=1`, 13.09M (78%) no tienen ningún `intelica_id` asignado (`NULL`, sin match de ninguna regla). Verificado que esto NO es específico de la población `token_flag=1` — la tasa general de `intelica_id NULL` en todo EBGR enero 2026 (sin filtrar por `token_flag`) es 82.6% (26.7M de 32.3M transacciones), incluso más alta que la del subconjunto `token_flag=1`. Es una característica preexistente de la cobertura de `mc_rules` para EBGR, no algo que el fix de hoy haya introducido o empeorado — queda fuera de este reproceso, sin investigar la causa.

**Comparativo EBGR contra legacy (2026-07-30) — confirma $0 impacto real del fix, cierra la validación end-to-end:** se corrió `glue-get-transaction` (`report_suffix=ebgr_202601`, rango completo enero 2026) contra el operational ya reprocesado y se comparó contra `analytics.report_transactions_ebgr_202601_tst` (`tst_files/reporting/run_ebgr.py`, con `JOB_NAME` corregido — apuntaba al nombre viejo `glue-test-1`, renombrado a `glue-get-transaction` el 2026-07-08). Se ajustó además `compare_ebgr.py`: el filtro `INCLUDE_BUSINESS_MODES={"I"}` (que excluía OUT de raíz, heredado de cuando EBGR no tenía datos OUT reales) se amplió a `{"I","A"}` para exponer el corte OUT como dimensión real en vez de ocultarlo — mismo `GROUP_KEYS_LIST` (19 dimensiones) que la última versión de SBSA, ahora con paridad completa.

**Resultado — población `I` (IN, la real de producción): idéntica al comparativo anterior (pre-fix)** — count_diff=-3, fee_diff=+167.44, sin cambios. Confirma con datos reales end-to-end lo que ya se había probado estructuralmente (cruce Athena de rutas activas de `token_flag`): el fix no movió nada en el tráfico real de EBGR. Población `A` (OUT): count_new=1798 vs count_legacy=0 — legacy no tiene OUT para EBGR en absoluto (coincide con que es cliente solo-emisor en producción), no es una discrepancia de datos.

**Intento de comparativo mas granular (jurisdiccion+negocio+intelica_id) — con bug metodologico propio, retractado:** se investigo si construir el mismo tipo de tabla "quien le quita a quien" que se uso para VI (`vi_jurisdiction_business_rules_hallazgo5bfix.py`) pero para MC usando `intelica_id` (ya que `ird` viene nativo en la data MC). Se encontraron swaps aparentemente grandes entre `intelica_id` adyacentes (ej. cientos de miles de transacciones). **Investigado y descartado como bug propio**: el script agrupaba por el bucket grueso `jurisdiction` (interregional/intraregional/off-us) + `intelica_id`, pero un mismo `intelica_id` matchea MUCHAS combinaciones `(region_country_code, ird)` a la vez (confirmado con `mc_rules`: `intelica_id=27` tiene filas para `BS/Q2`, `BB/75`, `BR/IA`, `KY/Q2`, `6/YW`, etc.) — sumar por `intelica_id` sin fijar `(region,ird)` exacto mezcla poblaciones distintas. El corte SI confiable y exacto (`interchange_rule`=`jurisdiction_assigned-ird`, ya en el comparativo `get_transaction.py` validado) muestra diferencias chicas (orden de cientos sobre millones: `9-YG` +100, `9-AS` +594, `9-61` +9) consistentes con el mecanismo ya documentado del envelope de 9 dígitos vs PAN exacto en `calculate_pre2()` (`gotchas.md`) — no relacionado al fix de hoy. Archivo dejado en `tst_files/reporting/ebgr/comparativo_mc_jurisdiccion_negocio_intelica_ebgr.md` con nota de advertencia al inicio, no citar sus números.

**Pendiente:** commitear (usuario, ya hecho para el código — ver `pending.md`); housekeeping de `file_control-02` en DynamoDB para los 143 archivos EBGR (mismo patrón aplicado a SBSA, baja urgencia); decidir si se investiga la tasa alta de `intelica_id NULL` en EBGR (hallazgo separado, no bloqueante); SBSA queda pendiente de definición con otras áreas antes de pasar a producción (tema cuotas).

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

**Commit:** hecho por el usuario junto con el resto de cambios de Visa/MC del 2026-07-30 (commit `f7b645b`).

**Reproceso EBGR VI IN completado y validado (2026-07-30):** encontrado al cerrar el reproceso MC de EBGR que el operational VI de EBGR no se reprocesaba desde 2026-06-28 — más de un mes antes de este refresh, así que nunca reflejó las reglas V37 ni los 3 fixes de arriba. Reprocesado `calculate→interchange→store` para EBGR/VI/IN enero 2026 (scope acordado con el usuario: solo IN, EBGR es mayormente/solo emisor) — calculate 105/105, interchange 62/62 (43 archivos solo-VSS correctamente excluidos, sin BASEII/SMS), store 105/105, sin fallos. Comparativo `get_transaction.py` contra legacy confirma: count exacto (4,051,482=4,051,482), transaction_amount exacto, `interchange_fees_amount` con residuo de -4.46 sobre $491,732.82 (-0.0009%, prácticamente igual al -1.30 de antes del reproceso) — impacto material nulo, reproceso cerrado.

**SBSA VI:** el reproceso masivo de 156 archivos (IN+OUT, ver Hallazgo 5b más abajo) corrió el mismo día del deploy de V37 y ya usa el `calculate.py`/`interchange.py` con los 3 fixes — pero esa validación se enfocó en el fix de ARDEF (Hallazgo 5b), no específicamente en confirmar matches reales de `token_requestor_id`/`cashback`/`settlement_flag` para SBSA. No se repitió el ejercicio de EBGR (comparativo dedicado post-V37) para SBSA — pendiente de bajo valor, sin decidir si vale la pena.

**Backups borrados (2026-07-31):** `visa_rules_backup_pre_v37_20260728/data.parquet` y `mc_rules_backup_pre_v23_20260730/data.parquet` eliminados de `s3-reference` — dados ambos refreshes ya validados con reprocesos reales y comparativos contra legacy sin impacto material, se decidió no mantenerlos indefinidamente como red de seguridad.

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
