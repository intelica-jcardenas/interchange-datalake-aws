# Pendientes del proyecto

Checklist vivo. Marcar `[x]` cuando se resuelva, con fecha y breve nota.
Última actualización: 2026-06-17.

---

## Pipeline Visa — bugs / validación

- [ ] **VI fees -18,679 EUR (-3.8%)** — diferencia residual de interchange fees en comparativo EBGR enero 2026 vs legacy. Componentes sospechosos: ATM JPY (rule 1055 vs 1065) + dirección de `exchange_value`. Ver `gotchas.md` → "matching incorrecto intelica_id ATM JPY".
- [ ] **ATM JPY rule 1055 vs 1065** — `glue-vi-interchange` asigna regla 1055 ("ATM AF") en vez de 1065 ("ATM AF JPN", fee_fixed=0.50 USD). 1 transacción, diff=-29.64. Requiere investigar qué campo en `visa_rules` diferencia ambas reglas.
- [ ] **`exchange_value` dirección** — validar si `exchange_rate/data.parquet` guarda `fee_ccy/source_ccy` (~1.08 para EUR→USD) o inverso (~0.926). Afecta fórmula de `calculate_fee_amounts` en `glue-vi-interchange`.
- [ ] **`calc_vss_aggregation_level`** — reescritura a join simple (sync 2026-06-12) pendiente re-run con archivo VSS real y validar que aparezcan valores `0` para hojas.
- [ ] **`glue-vi-interchange` reproceso masivo EBGR enero 2026** — `tst_files/reprocessing/reprocess_vi_interchange.py` creado 2026-06-11 pero NO ejecutado. Bloqueo: si se corre, el `operational/baseii_drafts` queda desincronizado hasta hacer un segundo pase de `reprocess_vi_store.py`. Ejecutar cuando los bugs de fees (ATM JPY + exchange_value) estén investigados, para hacer un único ciclo interchange → store.

---

## Pipeline Mastercard — gotchas vigentes en lmbd-mc-transform

- [ ] **Timeout multi-MTI** — procesa 4 MTIs secuencialmente, puede superar 400s si todos están presentes. Solución: invocar 1 Lambda por MTI desde Step Functions.
- [ ] **Sin chunking en MTIs 1442, 1740, 1644** — solo 1240 tiene chunking dinámico. Riesgo OOM en archivos grandes.
- [ ] **EphemeralStorage /tmp insuficiente** — default 512MB; `transform_ipm_1240` escribe Parquet completo en /tmp antes de subir a S3.
- [ ] **`DDB_MASTERCARD_FIELDS_TABLE` no declarada** — hardcodeada a `itl-0004-itx-dev-dynamo-mastercard_fields-02`; romperá en ambiente empresarial. Agregar a `config.json` y `env-vars.json`.
- [ ] **MC -3 filas en 2026-01-06** — comparativo vs legacy muestra -3 filas en esa fecha. Verificar `file_control-02` para esa fecha (puede ser un `status != DONE`).
- [ ] **Validación end-to-end `sfn-mc`** — pipeline MC completo desplegado, validación en curso con `itl-0004-itx-dev-intchg-02-sfn-mc`.

---

## Reporting (`get_transaction.py`)

- [ ] **`scheme_fees_amount`** — flujo no implementado, retorna 0.0. Pendiente diseño + implementación.
- [ ] **SMS transform** — skeleton con `# VERIFY`. Falta Parquet de muestra SMS para validar.
- [ ] **MC transform** — skeleton con `# VERIFY`. Pendiente datos limpios MC en operational.
- [ ] **MC TIMESTAMP(NANOS) en `get_transaction.py`** — `spark.read.parquet()` sobre `EBGR/MC/IPM_1240/` (138 archivos) falla con `AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))` porque el lector vectorizado ignora `nanosAsLong`. Fix propuesto sin probar: agregar `.config("spark.sql.parquet.enableVectorizedReader", "false")` a la SparkSession. Si funciona, eliminar los helpers `_read_operational_via_pyarrow`/`_widest_arrow_type`/`_align_table_to_schema`. Ver `gotchas.md` → "operational MC TIMESTAMP(NANOS)".
- [ ] **Escaneo NullType en SBSA, BTRLRO y vss_110-140** — antes de generar reportes para esos clientes/tipos, correr `tst_files/debug_scripts/scan_nulltype_columns.py` ajustando el prefijo S3. Si hay columnas NullType, reprocesar con `lmbd-vi-store` (mismo patrón que EBGR enero 2026).
- [ ] **Migrar fuente de tipos de cambio de `exchange_rate/` a `exchange-rates/`** — actualmente varios jobs leen de `s3-reference/exchange_rate/rate_date=YYYY-MM-DD/` (columnas: `currency_from, currency_to, exchange_value, brand`). La fuente definitiva será `exchange-rates/brand={Visa,MasterCard}/exchange_date=YYYY-MM-DD/` (columnas reales: `currency_from, currency_to, currency_from_code, currency_to_code, exchange_value`) — aún en desarrollo/completado por el equipo. Cuando `exchange-rates/` tenga cobertura completa de pares y fechas, actualizar: (1) `get_transaction.py` → `load_exchange_rates()`; (2) `glue-mc-calculate` y `glue-mc-interchange` si leen de esa fuente; (3) `glue-vi-interchange` si aplica. También adaptar el mapeo de nombres de columna en cada job.
- [ ] **`glue-test-3` (vi_data_quality.py)** — descargado 2026-06-17. Pendiente integrar en flujo / definir cómo se invoca.
- [ ] **`glue-test-4` (mc_data_quality.py)** — script en desarrollo local por el equipo. Cuando se suba a S3: descomentar en `scripts/sync-glue.ps1 $AllJobs` y ejecutar sync.

---

## Infraestructura AWS

- [ ] **Lambda huérfana `itx-interpreter`** — `itl-0004-itx-dev-intchg-02-itx-interpreter` (10240MB/900s, LastModified 2026-05-18, nunca invocada). Remanente de convención antigua. Eliminar tras confirmar que nada la referencia.
- [ ] **Log groups huérfanos** — `itx-ardef`, `itx-iar`, `itx-unzip` (sin función asociada). Eliminar.
- [ ] **Retención CloudWatch** — aplicar 30d a los grupos sin retención: `mc-clean`, `mc-exchange-rates`, `mc-extract`, `mc-iar`, `mc-interpreter`, `mc-store`, `mc-transform`, `vi-ardef`, `vi-exchange-rates`, `unzip`. Los grupos de producción (`archive-file`, `router`, `vi-*`, SFN Visa) tienen 60d inconsistente — normalizar a 30d también.
- [ ] **Rol IAM `itx-lambda-extract-role`** — `lmbd-vi-extract` comparte rol del router. Crear rol propio con permisos mínimos (S3 read/write staging, DynamoDB read visa-fields).
- [ ] **Rol IAM `itx-glue-crawler-ebgr-role`** — crawler Mastercard sin rol propio.
- [ ] **Mover scripts Glue MC** — de `itl-0004-itx-dev-poc-02-reference/` a `itl-0004-itx-dev-intchg-02-s3-reference/`.

---

## Nomenclatura y cleanup Glue

- [ ] **Renombrar `glue-test-1/3/4`** — a nombres con convención corporativa (ej. `glue-vi-mc-reporting`, `glue-vi-data-quality`, `glue-mc-data-quality`). Al renombrar, actualizar `$AllJobs` en `scripts/sync-glue.ps1`.
- [ ] **`glue-test-2`** — sin uso conocido. Verificar antes de eliminar.
- [ ] **Renombrar crawlers/databases `poc_*`** — existen 5 objetos con tercera convención de nombres. Inventario completo en `glue/GLUE_CATALOG_CREATION.md`.

---

## Documentación / cleanup legacy

- [ ] **Eliminar archivos con convención antigua** — `.env.example`, `step-functions/README.md`, `lambdas/router/README.md`, `infrastructure/deploy.sh`, `iam/README.md`, `CHANGELOG.md`. Recrear README.md en cada carpeta con la nomenclatura actual antes de eliminar.
- [ ] **`infrastructure/terraform/stepfunctions.tf`** — define 1 SFN (`itx_main_orchestrator`) pero AWS tiene 2 (`sfn-vi`, `sfn-mc`). Revisar y actualizar a 2 recursos antes de reescribir el archivo.

---

## Ambiente empresarial

- [ ] **Testing end-to-end en ambiente empresarial** — pendiente cuando el ambiente esté disponible.
