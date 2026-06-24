# Pendientes del proyecto

Checklist vivo. Marcar `[x]` cuando se resuelva, con fecha y breve nota.
Última actualización: 2026-06-23.

---

## Pipeline Visa — bugs / validación

- [ ] **VI fees -18,679 EUR (-3.8%)** — diferencia residual en comparativo EBGR enero 2026 vs legacy. Dos sub-investigaciones bloqueantes:
  - **ATM JPY rule 1055 vs 1065**: `glue-vi-interchange` asigna 1055 ("ATM AF") en vez de 1065 ("ATM AF JPN", fee_fixed=0.50 USD) para 1 transacción. Investigar qué campo en `visa_rules` diferencia ambas reglas.
  - **`exchange_value` dirección**: validar si `exchange_rate/data.parquet` guarda `fee_ccy/source_ccy` (~1.08 EUR→USD) o inverso (~0.926). Afecta fórmula de `calculate_fee_amounts` en `glue-vi-interchange`.
- [ ] **`glue-vi-interchange` reproceso masivo EBGR enero 2026** — `tst_files/reprocessing/reprocess_vi_interchange.py` listo pero NO ejecutado. Ejecutar una vez resueltas las dos sub-investigaciones de fees arriba; seguir con un pase de `reprocess_vi_store.py` para sincronizar operational.

---

## Reporting (`get_transaction.py`)

- [ ] **`scheme_fees_amount`** — flujo no implementado, retorna 0.0. Pendiente diseño + implementación. **Próximo:** implementar para cliente SBSA usando el mes de enero 2026 que se está procesando (archivos subidos 2026-06-23).
- [ ] **SMS transform** — skeleton con `# VERIFY`. Falta Parquet de muestra SMS para validar.
- [ ] **Migrar fuente de tipos de cambio de `exchange_rate/` a `exchange-rates/`** — la fuente definitiva será `exchange-rates/brand={Visa,MasterCard}/exchange_date=YYYY-MM-DD/` (columnas: `currency_from, currency_to, currency_from_code, currency_to_code, exchange_value`). Cuando tenga cobertura completa, actualizar: (1) `get_transaction.py` → `load_exchange_rates()`; (2) `glue-mc-calculate` y `glue-mc-interchange`; (3) `glue-vi-interchange` si aplica. Adaptar mapeo de nombres de columna en cada job.
- [ ] **`glue-test-3` (vi_data_quality.py)** — descargado 2026-06-17. Pendiente definir cómo se invoca e integrarlo al flujo.
- [ ] **`glue-test-4` (mc_data_quality.py)** — en desarrollo local por el equipo. Cuando se suba a S3: descomentar en `scripts/sync-glue.ps1 $AllJobs` y ejecutar sync.

---

## Infraestructura AWS

- [ ] **Rol IAM `itx-lambda-extract-role`** — `lmbd-vi-extract` comparte rol del router. Crear rol propio con permisos mínimos.
- [ ] **Rol IAM `itx-glue-crawler-ebgr-role`** — crawler Mastercard sin rol propio.
---

## Documentación / cleanup legacy

- [ ] **Eliminar archivos con convención antigua** — `.env.example`, `step-functions/README.md`, `lambdas/router/README.md`, `infrastructure/deploy.sh`, `iam/README.md`, `CHANGELOG.md`. Recrear README.md por carpeta con nomenclatura actual antes de eliminar.
- [ ] **`infrastructure/terraform/stepfunctions.tf`** — define 1 SFN (`itx_main_orchestrator`) pero AWS tiene 2 (`sfn-vi`, `sfn-mc`). Actualizar a 2 recursos antes de reescribir.

---

## Cleanup de recursos obsoletos (para equipo de infra)

Inventario detallado con orden de ejecución: `tst_files/ticket_cleanup_itx_dev.txt`. Listo para ejecutar (2026-06-23).

Inventario de objetos a eliminar por convención antigua o sin uso. Organizado por servicio.

### AWS Glue

**Crawlers — eliminar:**
- `itl-0004-itx-dev-intchg-02-crawler-staging` — convención antigua (guiones), nunca ejecutado, apunta a todo `s3-staging/`
- `itl-0004-itx-dev-intchg-02-crawler-reference` — convención antigua (guiones), apunta a `s3-reference/exchange-rates/`, escribe a database `poc_itx_reference`

**Databases — eliminar:**
- `itl_0004_itx_dev_poc_ebgr_visa_staging` — convención `poc_*`, nunca populada
- `itl_0004_itx_dev_poc_interchange_analytics` — convención `poc_*`, sin crawler asociado
- `itl_0004_itx_dev_poc_itx_reference` — convención `poc_*`, usada por `crawler-reference` (a eliminar)

**Jobs — renombrar** (pendiente de coordinación, actualizar `$AllJobs` en `sync-glue.ps1` al renombrar):
- `itl-0004-itx-dev-intchg-02-glue-test-1` → `itl-0004-itx-dev-intchg-02-glue-vi-mc-reporting`
- `itl-0004-itx-dev-intchg-02-glue-test-3` → `itl-0004-itx-dev-intchg-02-glue-vi-data-quality`
- `itl-0004-itx-dev-intchg-02-glue-test-4` → `itl-0004-itx-dev-intchg-02-glue-mc-data-quality`

### AWS S3

**Buckets POC — eliminar** (convención `itl-0004-itx-dev-poc-02-*`, reemplazados por `intchg-02`):
- `itl-0004-itx-dev-poc-02-analytics`
- `itl-0004-itx-dev-poc-02-archive`
- `itl-0004-itx-dev-poc-02-landing`
- `itl-0004-itx-dev-poc-02-operational`
- `itl-0004-itx-dev-poc-02-staging`
- `itl-0004-itx-dev-poc-02-reference` — scripts Glue MC ya migrados al bucket oficial (verificado 2026-06-18), puede eliminarse

### AWS Lambda

**Lambda — eliminar:**
- `itl-0004-itx-dev-intchg-02-itx-interpreter` — convención antigua (`itx-*`), nunca invocada (LastModified 2026-05-18), reemplazada por `lmbd-mc-interpreter`
- `itl-0004-itx-dev-mastercard-exchange-rates` — convención sin `intchg-02`, reemplazada por `lmbd-mc-exchange-rates` (LastModified 2026-05-15)
- `itl-0004-itx-dev-poc-testhello` — test POC descartable

### AWS CloudWatch Logs

**Log groups huérfanos — eliminar** (función Lambda ya no existe):
- `/aws/lambda/itl-0004-itx-dev-intchg-02-itx-ardef`
- `/aws/lambda/itl-0004-itx-dev-intchg-02-itx-iar`
- `/aws/lambda/itl-0004-itx-dev-intchg-02-itx-unzip`

**Log groups — eliminar junto con su Lambda** (se vuelven huérfanos al borrar la función):
- `/aws/lambda/itl-0004-itx-dev-intchg-02-itx-interpreter` (60d) — junto a Lambda `itx-interpreter`
- `/aws/lambda/itl-0004-itx-dev-mastercard-exchange-rates` (sin retención) — junto a Lambda `mastercard-exchange-rates`
- `poc-testhello`: sin log group (nunca se invocó)

**Log groups — aplicar retención 30d** (actualmente sin retención):
- `mc-clean`, `mc-exchange-rates`, `mc-extract`, `mc-iar`, `mc-interpreter`, `mc-store`, `mc-transform`
- `vi-ardef`, `vi-exchange-rates`, `unzip`

**Log groups — normalizar a 30d** (actualmente 60d inconsistente):
- `archive-file`, `router`, `vi-clean`, `vi-extract`, `vi-store`, `vi-transform`, SFN Visa

---

## Ambiente empresarial

- [ ] **Testing end-to-end en ambiente empresarial** — pendiente cuando el ambiente esté disponible.
