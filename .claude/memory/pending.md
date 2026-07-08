# Pendientes del proyecto

Checklist vivo — solo tareas activas. Lo resuelto se documenta en `decisions.md`/`gotchas.md` (o en la memoria de usuario correspondiente) y se borra de aquí.
Última actualización: 2026-07-08.

---

## Pipeline Visa — residuales conocidos (bajos, aceptados)

- [ ] **ATM NO AF + ATM DCC NO AF (SBSA):** 2,015 trx "ATM NO AF" (+59,988 ZAR) + 739 "ATM DCC NO AF" (+3,590 ZAR), count_legacy=0 en ambas. Total ~+63.6K ZAR de 179.7M (0.035%). Investigar qué regla asignaba legacy.
- [ ] **ATM JPY rule 1055 vs 1065** (EBGR, 1 transacción): `glue-vi-interchange` asigna 1055 en vez de 1065 — fee_fixed=0.50 USD faltante. Investigar campo diferenciador en `visa_rules`.

---

## Pipeline Mastercard — cambios en progreso (sin commitear)

- [ ] **glue-mc-interchange — rewrite de `calculate_mastercard_fee_pyspark` (moneda del fee):** cambio local sin commitear en `glue/scripts/mastercard/interchange/interchange.py`. Pasa `calculated_fee` de un esquema de 2 pasos a siempre estar en `trx_ccy` (DE_49), 1 paso. Falta: subir a S3, ejecutar `glue-mc-interchange` (`tst_files/reprocessing/reprocess_mc_interchange.py`), validar contra legacy, limpiar columna `settlement_currency_u` sin uso, revisar si reabre los residuales ATM JPY/ATM NO AF de arriba. Detalle en memoria de usuario `mc_interchange_fee_currency_rewrite.md`.

---

## Reporting (`get_transaction.py`)

- [ ] **`scheme_fees_amount` (cuotas)** — retorna 0.0 fijo. Una vez validado `--mode read` de `glue-scheme-fee`, debería leer `unitary_scheme_fee_cost` desde `s3-analytics/{client}/scheme_fee/final/{report_month}/detail/`.
- [ ] **SMS transform** — skeleton con `# VERIFY`. Falta Parquet de muestra SMS real para validar campos (distinto del SMS de `scheme_fee.py`, ese ya está validado).

---

## Scheme Fee (cuotas) — `glue-scheme-fee`

`--mode generate` **validado end-to-end** (2026-07-08): auditoría campo por campo completa contra el legacy (`getquery.py`/`managment.py`), residuales explicados y aceptados (VISA: INNER JOIN de ARDEF en legacy que nuestro LEFT JOIN no replica, +0.03%; MC: swap DE_4/DE_5 en transacciones cross-currency, decisión de mantenerlo — ver `decisions.md`). Historia completa de diseño, bugs encontrados/corregidos y validaciones en memoria de usuario `scheme_fee_job_design.md`.

- [ ] **Probar el ciclo completo `--mode read`** con un CSV de vuelta simulado (rellenar `txn_sfc`/`est_sch_fee_amt` a mano) para validar el join por `app_id` y la propagación al detalle.

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
