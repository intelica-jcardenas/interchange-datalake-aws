# Pendientes del proyecto

Checklist vivo — solo tareas activas. Lo resuelto se documenta en `decisions.md`/`gotchas.md` (o en la memoria de usuario correspondiente) y se borra de aquí.
Última actualización: 2026-07-10.

---

## Pipeline Visa — residuales conocidos (bajos, aceptados)

- [ ] **ATM NO AF + ATM DCC NO AF (SBSA):** 2,015 trx "ATM NO AF" (+59,988 ZAR) + 739 "ATM DCC NO AF" (+3,590 ZAR), count_legacy=0 en ambas. Total ~+63.6K ZAR de 179.7M (0.035%). Investigar qué regla asignaba legacy. Aún no revisado (2026-07-09) — queda pendiente para después.
- [ ] **ATM JPY rule 1055 vs 1065** (EBGR, 1 transacción): `glue-vi-interchange` asigna 1055 en vez de 1065 — fee_fixed=0.50 USD faltante. Investigar campo diferenciador en `visa_rules`.

---

## Pipeline Mastercard — residual conocido (bajo, aceptado por ahora)

- [ ] **jurisdiction_code NULL-vs-off-us (SBSA, MC):** comparativo `get_transaction.py` agrupado por `jurisdiction_code` muestra `NULL`=249 (nuevo) vs 7,326 (legacy) y `off-us`=+6,812 de más en el nuevo — legacy arma `dh_mastercard_calculated_field_*` con INNER JOIN contra IAR (deja esas ~6,800 transacciones sin clasificar), nuestro LEFT JOIN sí las clasifica. Causa parcial confirmada y cuantificada: `calculate_pre2()` usa un envelope de 9 dígitos del PAN (no el PAN real) para el range-join IAR, dando falsos positivos cuando un prefijo de 9 dígitos tiene sub-rangos IAR angostos con huecos — mide ~11% del gap (~764 de 6,812 transacciones, proyectado). Descartado: diferencia de contenido en la tabla IAR (idéntica, 186,454 rangos activos en ambos sistemas). Confirmado que el fix de máscara de PAN y el fix de schema Arrow (ambos reprocesados 2026-07-09) NO tocan este gap — comparativo regenerado dio byte-idéntico al anterior. **89% del gap (~6,000 transacciones) sigue sin causa identificada.** Impacto en dólares mínimo (`interchange_fees_amount_diff` MC = +10,001.28 ZAR de 221.9M, 0.0045%). Detalle completo → `.claude/memory/gotchas.md`. Investigación pausada por bajo impacto — si se retoma: comparar dedup IAR fila-por-fila (no solo conteo) para el residual chico de intra/interregional (+202/+63); investigar el 89% restante.

---

## Pipeline Mastercard — cambios en progreso (sin commitear)

- [ ] **glue-mc-interchange — rewrite de `calculate_mastercard_fee_pyspark` (moneda del fee):** cambio local sin commitear en `glue/scripts/mastercard/interchange/interchange.py`. Pasa `calculated_fee` de un esquema de 2 pasos a siempre estar en `trx_ccy` (DE_49), 1 paso. Falta: subir a S3, ejecutar `glue-mc-interchange` (`tst_files/reprocessing/reprocess_mc_interchange.py`), validar contra legacy, limpiar columna `settlement_currency_u` sin uso, revisar si reabre los residuales ATM JPY/ATM NO AF de arriba. Detalle en memoria de usuario `mc_interchange_fee_currency_rewrite.md`.

---

## Reporting (`get_transaction.py`)

- [x] **`scheme_fees_amount` (cuotas) — unificación get_transaction.py + scheme_fee.py — VALIDADO END-TO-END (2026-07-10) con costos dummy.** Falta solo validar con costos REALES (ver pendiente al final).

  **Diseño:** `load_scheme_fee_costs(client_id, start_date, analytics_bucket)` en `get_transaction.py` lee `s3-analytics/{client}/scheme_fee/final/{report_month}/detail/` (`report_month` derivado de `start_date`, asume rango dentro de un único mes calendario), traduce `app_hash_file→file_id`, `app_id→row_id`, `business_mode_id→business_mode_code` (A/I), `unitary_scheme_fee_cost→scheme_fees_amount`. En `process_client_range()`, solo si `--scheme_fee=true`: `LEFT JOIN` por `(file_id, row_id, business_mode_code)`, `fillna(0.0)` si no hay match (decisión deliberada — legacy deja `NULL`, ver razón abajo).

  **Diseño del join VALIDADO contra el SQL legacy real** (no solo contra los scripts EC2 de scheme fee): `sql/get_visa_base_ii_transactions.sql`, `get_visa_sms_transactions.sql`, `get_mastercard_transactions.sql` (invocados desde `sql/generate_transaction_table.sql`, el SP real). Legacy hace `LEFT JOIN mh_transaction_scheme_fee S1 ON app_hash_file+app_id+transaction_brand` (sin `table_description`, genera fan-out temporal en transacciones on-us con duplicado) → filtra `table_description <> '...ON-US DUP...'` para quedarse con la original → segundo join separado, filtrado a `table_description='...ON-US DUP...'`, para el costo del duplicado. Nuestro join `(file_id, row_id, business_mode_code)` es equivalente matemático (business_mode_id es único por fila, máx. 2 filas por file_id+row_id) — un solo join en vez de join+where+join. Confirmado también: el gating `dup_on_us and SCHEME_FEE` coincide literal con `duplicate_on_us_flag IS TRUE AND param_scheme_fee IS TRUE` de legacy; legacy tampoco niega `scheme_fees_amount` en reversales (solo `transaction_amount`/`interchange_fees_amount`); `transaction_brand` en el join legacy es redundante en el pipeline nuevo porque `file_id`(=content_hash) ya es único por archivo físico.

  **`table_description` corregido a los literales exactos del legacy** (`getquery.py`) en `scheme_fee.py`: VISA BASEII `"VISA ACQ"`/`"VISA ISS"` (antes: `"VISA"` fijo, sin distinguir), dup `"VISA ON-US DUP (ACQ TO ISS)"`; SMS `"VISA SMS"` (sin cambio) / dup `"VISA ON-US DUP (SMS TO ISS)"`; MasterCard `"MASTERCARD ISS AND ACQ"` (antes `"MasterCard"`) / dup `"MASTERCARD ON-US DUP (ACQ TO ISS)"`. `_apply_duplicate_on_us()` recibe el literal exacto por brand como parámetro. No afecta `GROUP_DIMS` (el reporte agregado no cambia, solo el detalle).

  **Bug real encontrado y corregido:** `get_client_config()` en `get_transaction.py` casteaba mal `dup_on_us_visa`/`dup_on_us_mc` con `bool(item.get(...))` — `bool("FALSE")` es `True` en Python. Afectaba a EBGR/NXGR/DEMO (guardan el flag como string literal `"FALSE"` en DynamoDB), enmascarado hasta ahora porque el default de `--scheme_fee` en AWS es `"false"`. Corregido con el mismo parseo explícito que ya usaba `scheme_fee.py`. De paso corregido `report_currency` (afecta a BTRLRO, campo presente pero vacío).

  **Validación real ejecutada (2026-07-10), SBSA/202601** — ambos scripts subidos a S3 y corridos de punta a punta: `scheme_fee.py --mode generate --force true` (452,531 filas de reporte) → CSV IN simulado con costos dummy (`txn_sfc`=0.25% de `txn_amt`) → `--mode read` (0 filas con costo NULL) → `get_transaction.py --scheme_fee true` (120,147,824 filas, 99.9994% con `scheme_fees_amount` poblado — solo 758 filas sin match). Comparado agregadamente contra `analytics.report_transactions_sbsa_202601_tst_sf` (tabla legacy real, corrida por el usuario con `param_scheme_fee=true`, costos REALES):
  - **MC: count match exacto** (82,358,209=82,358,209), `transaction_amount` exacto, `interchange_fees_amount` diff=+10,001.28 (+0.0045%) — **idéntico al residual ya conocido y aceptado antes de este trabajo**, sin relación con scheme_fee.
  - **VI: diffs 100% explicados por residuales ya conocidos, ninguno nuevo.** `count` diff=+38,155, de los cuales 37,616 son el gap ya documentado de `jurisdiction_code=""` (INNER JOIN de ARDEF en legacy). `interchange_fees_amount` diff=+68,285.41 (+0.038%), concentrado en `jurisdiction=interregional` — consistente con el residual ya conocido de ATM NO AF/ATM DCC NO AF.
  - `scheme_fees_amount` no es comparable en monto (dummy vs costos reales de legacy) — la validación de VALOR real queda pendiente (ver abajo).
  - **Conclusión: el mecanismo de duplicado on-us + join de costos no introduce ningún problema nuevo** — todos los diffs observados ya existían y estaban aceptados antes de activar scheme_fee.
  Scripts (`tst_files/`, gitignored): `scheme_fee_reports/run_scheme_fee_union_test.py` (orquestador AWS), `scheme_fee_reports/compare_scheme_fee_union_sbsa.py` (comparativa vs legacy). Reporte: `scheme_fee_reports/union_test/comparativo_scheme_fee_union_sbsa.md`. Muestras Parquet refrescadas en `scheme_fee_parquet_samples/{generate_state,read_final}/`.

  **Pendiente real que queda:**
  - Validar con costos REALES (no dummy) una vez que haya un ciclo real `--mode read` con el CSV devuelto por el equipo externo.
  - Reactivar SMS en `get_transaction.py` (comentado deliberadamente, decisión del usuario 2026-07-10) — mientras tanto, los `scheme_fees_amount` de las filas SMS de `scheme_fee.py` (que sí tiene SMS activo) no se propagan a ningún lado.
  - Commitear a git todos los cambios de esta sesión (ver ítem "Commitear a git" en la sección Scheme Fee más abajo).
- [x] **REACTIVAR BASEII y MC en `process_client_range()`** (2026-07-09) — hecho, código y S3 sincronizados.
- [x] **SMS transform — activado y validado (2026-07-09)** — `count` y `transaction_amount` cuadran exacto contra `get_visa_sms_transactions()` real (SBSA enero 2026): 1,472,615=1,472,615, $1,059,497,522.78 exacto. Bug real encontrado y corregido (`xr3_rate` hardcodeado a USD para el fallback de `cryptogram_amount`, revertido un intento fallido de usar `xr2_rate` en fees que causaba doble conversión). Detalle en `gotchas.md`.
- [ ] **SMS vuelve a estar comentado en `process_client_range()`** (2026-07-09, a pedido del usuario) — mientras se trabaja en la unificación `get_transaction.py`+`scheme_fee`. Reactivar cuando se retome (el fix ya está en el código, solo comentado). No se corrió la validación final BASEII+SMS+MC juntos (el job se canceló) — pendiente confirmar que la unión de los 3 frames no rompe nada cuando se reactive.
- [ ] **`interchange_fees_amount` de SMS +60.55% de más, concentrado 100% en `transaction_type_id=22`** (ATM cash withdrawal) — no es bug de `get_transaction.py` (ya validado), es del cálculo de fee en `glue-vi-interchange` para el `type_record` SMS. Ver `gotchas.md` para el desglose y candidatos de causa. Pendiente investigar y reprocesar de ser necesario — no bloquea el resto del reporting.

---

## Scheme Fee (cuotas) — `glue-scheme-fee`

`--mode generate` **validado end-to-end** (2026-07-08): auditoría campo por campo completa contra el legacy (`getquery.py`/`managment.py`), residuales explicados y aceptados (VISA: INNER JOIN de ARDEF en legacy que nuestro LEFT JOIN no replica, +0.03%; MC: swap DE_4/DE_5 en transacciones cross-currency, decisión de mantenerlo — ver `decisions.md`). Historia completa de diseño, bugs encontrados/corregidos y validaciones en memoria de usuario `scheme_fee_job_design.md`.

`--mode read` **validado end-to-end** (2026-07-08). Se encontraron y corrigieron 2 bugs reales durante la validación:
1. Join `detail_df.join(cost_by_group, on=GROUP_DIMS, how="left")` en `update_report_and_propagate()` usaba igualdad estándar de Spark (`NULL == NULL` → false), dejando 3.2% de las filas de detalle (3,895,101 de 121,072,180) sin costo propagado. Fix: `eqNullSafe` columna por columna.
2. Ese mismo fix reveló un segundo bug: `report_df.toPandas()` en `run_generate()` convertía `range_program_id` (int32, con nulls) a `float64` con NaN — al reconvertir a Spark, ese NaN se persistía como valor real (no NULL), rompiendo el `eqNullSafe` para esos 755 grupos (40,302 filas). Fix: sanitizar NaN→None antes de `spark.createDataFrame()`.

De paso se corrigió también el `app_id` de Mastercard en `transform_mastercard_scheme_fee()` (`F.monotonically_increasing_id()` → `F.col("ref_id")`), necesario para poder cruzar el costo de scheme fee contra `get_transaction.py` a nivel de transacción individual.

Ambos fixes desplegados a S3 y **re-validados con una corrida real completa** (`--mode generate --force true` + `--mode read`, SBSA/202601): 0 filas con costo NULL (antes 40,302), y 5/5 transacciones MC de muestra cruzadas exitosamente contra el operational real por `content_hash`+`ref_id`. Detalle completo en memoria de usuario `scheme_fee_job_design.md` y en `.claude/memory/scheme_fee_generate_read_pipeline.md`.

- [ ] **Commitear a git** — `scheme_fee.py` (2 fixes de `--mode read` + `table_description` exacto a legacy), `lambdas/visa/clean` + `lambdas/mastercard/clean` (fix máscara account_number/pan_de_2), `lambdas/mastercard/store` (fix schema CAL/ITX), `glue/scripts/mastercard/calculate/calculate.py` (fix schema Arrow en `save_parquet()`, causa raíz real del problema de `mc-store`), `glue/scripts/reports/get_transaction/get_transaction.py` (fix file_id=content_hash + fix `get_client_config()` dup_on_us/report_currency + join `scheme_fees_amount` nuevo). Todos aplicados, desplegados y validados con reproceso real (SBSA/202601) — ninguno commiteado (decisión del usuario, se encarga él).
- [ ] **Retirar `.claude/memory/scheme_fee_generate_read_pipeline.md`** (marcado como temporal) una vez que Scheme Fee se dé por cerrado — fusionar lo que siga siendo relevante a `decisions.md`/`gotchas.md`.

---

## Infraestructura AWS

- [ ] **Rol IAM `itx-lambda-extract-role`** — `lmbd-vi-extract` comparte rol del router. Crear rol propio con permisos mínimos.
- [ ] **Rol IAM `itx-glue-crawler-ebgr-role`** — crawler Mastercard sin rol propio.
- [ ] **Agregar `s3-reference/currency/` como target** a un crawler existente (ej. `itl_0004_itx_dev_02_glue_crawler_exchange_rates`) o crear uno dedicado — baja prioridad. El job `glue-exchange-rates` lee el contenido en vivo (no depende del catálogo), así que solo protege contra un futuro cambio de schema en `currency/`.

---

## Documentación / cleanup legacy

- [ ] **Eliminar archivos con convención antigua** — `.env.example`, `step-functions/README.md`, `lambdas/router/README.md`, `infrastructure/deploy.sh`, `iam/README.md`, `CHANGELOG.md`. Recrear README.md por carpeta con nomenclatura actual antes de eliminar.
- [ ] **`infrastructure/terraform/stepfunctions.tf`** — define 1 SFN (`itx_main_orchestrator`) pero AWS tiene 2 (`sfn-vi`, `sfn-mc`). Actualizar a 2 recursos antes de reescribir.
- [ ] **Renombrar crawlers/databases Glue con prefijo `itx-` consistente** — los 16 objetos planeados en `glue/GLUE_CATALOG_CREATION.md` existen pero con nombres reales que omiten `intchg`. Ver sección "Estado de verificación" de ese archivo.

---

## Ambiente empresarial

- [ ] **Testing end-to-end en ambiente empresarial** — pendiente cuando el ambiente esté disponible.
