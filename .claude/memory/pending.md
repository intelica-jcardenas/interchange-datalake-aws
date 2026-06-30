# Pendientes del proyecto

Checklist vivo. Marcar `[x]` cuando se resuelva, con fecha y breve nota.
Última actualización: 2026-06-30 (EBGR + SBSA validados, get_transaction cerrado).

---

## Pipeline Visa — residuales conocidos (bajos, aceptados)

- [ ] **ATM NO AF + ATM DCC NO AF (SBSA) — clasificación residual:** 2,015 trx "ATM NO AF" (+59,988 ZAR) + 739 "ATM DCC NO AF" (+3,590 ZAR), count_legacy=0 en ambas. Total ~+63.6K ZAR de 179.7M (0.035%). Investigar qué regla asignaba legacy (NULL/catch-all o filtro previo).
- [ ] **ATM JPY rule 1055 vs 1065** (EBGR, 1 transacción): `glue-vi-interchange` asigna 1055 en vez de 1065 — fee_fixed=0.50 USD faltante. Investigar campo diferenciador en `visa_rules`. Impacto monetario subsumed en el -1.30 EUR de EBGR validado.

---

## Reporting (`get_transaction.py`)

- [ ] **`scheme_fees_amount` (cuotas)** — flujo no implementado, retorna 0.0. **Próximo gran paso:** diseño + implementación.
- [ ] **SMS transform** — skeleton con `# VERIFY`. Falta Parquet de muestra SMS real para validar campos.
- [ ] **Migrar fuente de tipos de cambio de `exchange_rate/` a `exchange-rates/`** — cuando `exchange-rates/brand={Visa,MasterCard}/exchange_date=YYYY-MM-DD/` tenga cobertura completa, actualizar `load_exchange_rates()` en `get_transaction.py`, `glue-mc-calculate`, `glue-mc-interchange` y `glue-vi-interchange`.
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

### AWS Glue

**Crawlers — eliminar:**
- `itl-0004-itx-dev-intchg-02-crawler-staging` — convención antigua (guiones), nunca ejecutado, apunta a todo `s3-staging/`
- `itl-0004-itx-dev-intchg-02-crawler-reference` — convención antigua (guiones), apunta a `s3-reference/exchange-rates/`, escribe a database `poc_itx_reference`

**Databases — eliminar:**
- `itl_0004_itx_dev_poc_ebgr_visa_staging` — convención `poc_*`, nunca populada
- `itl_0004_itx_dev_poc_interchange_analytics` — convención `poc_*`, sin crawler asociado
- `itl_0004_itx_dev_poc_itx_reference` — convención `poc_*`, usada por `crawler-reference` (a eliminar)

**Jobs — renombrar** (actualizar `$AllJobs` en `sync-glue.ps1` al renombrar):
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
- `itl-0004-itx-dev-poc-02-reference` — scripts Glue MC ya migrados al bucket oficial (verificado 2026-06-18)

### AWS Lambda

**Lambda — eliminar:**
- `itl-0004-itx-dev-intchg-02-itx-interpreter` — convención antigua (`itx-*`), nunca invocada, reemplazada por `lmbd-mc-interpreter`
- `itl-0004-itx-dev-mastercard-exchange-rates` — convención sin `intchg-02`, reemplazada por `lmbd-mc-exchange-rates`
- `itl-0004-itx-dev-poc-testhello` — test POC descartable

### AWS CloudWatch Logs

**Log groups huérfanos — eliminar:**
- `/aws/lambda/itl-0004-itx-dev-intchg-02-itx-ardef`
- `/aws/lambda/itl-0004-itx-dev-intchg-02-itx-iar`
- `/aws/lambda/itl-0004-itx-dev-intchg-02-itx-unzip`
- `/aws/lambda/itl-0004-itx-dev-intchg-02-itx-interpreter` — junto a Lambda `itx-interpreter`
- `/aws/lambda/itl-0004-itx-dev-mastercard-exchange-rates` — junto a Lambda `mastercard-exchange-rates`

**Log groups — aplicar retención 30d** (actualmente sin retención):
- `mc-clean`, `mc-exchange-rates`, `mc-extract`, `mc-iar`, `mc-interpreter`, `mc-store`, `mc-transform`
- `vi-ardef`, `vi-exchange-rates`, `unzip`

**Log groups — normalizar a 30d** (actualmente 60d inconsistente):
- `archive-file`, `router`, `vi-clean`, `vi-extract`, `vi-store`, `vi-transform`, SFN Visa

---

## Ambiente empresarial

- [ ] **Testing end-to-end en ambiente empresarial** — pendiente cuando el ambiente esté disponible.
