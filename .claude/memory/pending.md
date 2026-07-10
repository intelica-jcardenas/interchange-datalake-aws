# Pendientes del proyecto

Checklist vivo — solo tareas activas. Lo resuelto se documenta en `decisions.md`/`gotchas.md` (o en la memoria de usuario correspondiente) y se borra de aquí.
Última actualización: 2026-07-09.

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

- [ ] **`scheme_fees_amount` (cuotas) — unificación get_transaction.py + scheme_fee.py (a retomar)** — retorna 0.0 fijo. Una vez validado `--mode read` de `glue-scheme-fee`, debería leer `unitary_scheme_fee_cost` desde `s3-analytics/{client}/scheme_fee/final/{report_month}/detail/`.
  **Llave de unión propuesta:** `file_id` (=content_hash) + `row_id` (=record/ref_id) + `business_mode_code`/`table_description` en ambos lados.
  **Pendiente de implementar antes de la unión (diseño, no código):** ni `get_transaction.py` ni `scheme_fee.py` distinguen la fila duplicada on-us de la original en su llave — `_apply_duplicate_on_us()` en ambos archivos solo cambia `business_mode_code`/`business_mode_id` (A→I / ACQUIRING→ISSUING), dejando `file_id`+`row_id` idénticos entre el original y el duplicado. Legacy resuelve esto con `table_description` (columna que YA existe en `scheme_fee.py` — línea ~770/971/1169, valores `"VISA"`/`"VISA SMS"`/`"MasterCard"` — pero `_apply_duplicate_on_us()` de `scheme_fee.py` no le asigna un valor distinto al duplicado). Propuesta: replicar el patrón legacy asignando al duplicado un `table_description` propio (ej. `"VISA ON-US DUP (ACQ TO ISS)"`, `"VISA SMS ON-US DUP (SMS TO ISS)"`, `"MasterCard ON-US DUP (ACQ TO ISS)"`) en `_apply_duplicate_on_us()` de `scheme_fee.py`, para que el join contra `get_transaction.py` no dependa implícitamente de `business_mode_id` para desambiguar. Decisión pendiente de confirmar mañana antes de implementar.
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

- [ ] **Commitear a git** — `scheme_fee.py` (2 fixes), `lambdas/visa/clean` + `lambdas/mastercard/clean` (fix máscara account_number/pan_de_2), `lambdas/mastercard/store` (fix schema CAL/ITX), `glue/scripts/mastercard/calculate/calculate.py` (fix schema Arrow en `save_parquet()`, causa raíz real del problema de `mc-store`), `glue/scripts/reports/get_transaction/get_transaction.py` (fix file_id=content_hash). Todos aplicados, desplegados y validados con reproceso real (SBSA/202601, 104/104 OK) — ninguno commiteado (decisión del usuario, se encarga él).
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
