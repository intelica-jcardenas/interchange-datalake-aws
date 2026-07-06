# Pendientes del proyecto

Checklist vivo. Marcar `[x]` cuando se resuelva, con fecha y breve nota.
Última actualización: 2026-07-03 (primera corrida real de scheme_fee.py --mode generate SUCCEEDED contra SBSA enero 2026, tras corregir 2 bugs encontrados en el camino; falta revisar contenido del CSV y probar --mode read).

---

## Pipeline Visa — residuales conocidos (bajos, aceptados)

- [ ] **ATM NO AF + ATM DCC NO AF (SBSA) — clasificación residual:** 2,015 trx "ATM NO AF" (+59,988 ZAR) + 739 "ATM DCC NO AF" (+3,590 ZAR), count_legacy=0 en ambas. Total ~+63.6K ZAR de 179.7M (0.035%). Investigar qué regla asignaba legacy (NULL/catch-all o filtro previo).
- [ ] **ATM JPY rule 1055 vs 1065** (EBGR, 1 transacción): `glue-vi-interchange` asigna 1055 en vez de 1065 — fee_fixed=0.50 USD faltante. Investigar campo diferenciador en `visa_rules`. Impacto monetario subsumed en el -1.30 EUR de EBGR validado.

---

## Pipeline Mastercard — cambios en progreso (sin commitear)

- [ ] **glue-mc-interchange — rewrite de `calculate_mastercard_fee_pyspark` (moneda del fee):** cambio local sin commitear en `glue/scripts/mastercard/interchange/interchange.py` (detectado 2026-07-01, no está en git log). Pasa de un esquema de 2 pasos (`calculated_fee` en `rule_currency` + `calculated_fee_settlement` convertido a `settlement_currency`, esta última calculada pero no usada aguas abajo) a un esquema de 1 paso: `calculated_fee` siempre en `trx_ccy` (DE_49), tanto para IN como para OUT. Motivado por el hallazgo de `tst_files/mc_fee_currency/test_mc_interchange_fee_currency.py` (el ITX escribía el fee en `rate_currency`, no en una moneda útil para reportería). Falta: subir el script a S3, ejecutar `glue-mc-interchange` (reproceso disponible en `tst_files/reprocessing/reprocess_mc_interchange.py`), validar contra legacy, limpiar la columna `settlement_currency_u` que quedó sin uso, y revisar si esto reabre los residuales ATM JPY/ATM NO AF de arriba (ambos tocan moneda/fee). Detalle completo en memoria de usuario `mc_interchange_fee_currency_rewrite.md`.

---

## Reporting (`get_transaction.py`)

- [ ] **`scheme_fees_amount` (cuotas)** — flujo no implementado, retorna 0.0. Diseño del proceso completo en marcha, ver sección "Scheme Fee" abajo — una vez validado el nuevo job `glue-scheme-fee`, este campo debería poder leer `unitary_scheme_fee_cost` desde `s3-analytics/{client}/scheme_fee/final/{report_month}/detail/` en vez de retornar 0.0 fijo.
- [ ] **SMS transform** — skeleton con `# VERIFY`. Falta Parquet de muestra SMS real para validar campos.
- [ ] **Migrar fuente de tipos de cambio de `exchange_rate/` a `exchange-rates/`** — cuando `exchange-rates/brand={Visa,MasterCard}/exchange_date=YYYY-MM-DD/` tenga cobertura completa, actualizar `load_exchange_rates()` en `get_transaction.py`, `glue-mc-calculate`, `glue-mc-interchange` y `glue-vi-interchange`.
- [ ] **`glue-test-3` (vi_data_quality.py)** — descargado 2026-06-17. Pendiente definir cómo se invoca e integrarlo al flujo.
- [ ] **`glue-test-4` (mc_data_quality.py)** — en desarrollo local por el equipo. Cuando se suba a S3: descomentar en `scripts/sync-glue.ps1 $AllJobs` y ejecutar sync.

---

## Scheme Fee (cuotas) — diseñado 2026-07-01, infraestructura lista 2026-07-03, sin validar aún

Réplica del módulo legacy EC2 (`tst_files/scheme_Fee_legacy_scripts/`) en `glue/scripts/reports/scheme_fee/scheme_fee.py`. Corre en un Glue job propio, `itl-0004-itx-dev-intchg-02-glue-scheme-fee` (no reusa `glue-test-2`, que mantiene su función de exchange rates y se renombra a `glue-exchange-rates` — ver "Cleanup de recursos obsoletos" abajo). Detalle completo del diseño, mapeo columna-a-columna contra el legacy y simplificaciones conocidas en memoria de usuario `scheme_fee_job_design.md`.

**Infraestructura desplegada — HECHO 2026-07-03:**
- [x] Bucket `itl-0004-itx-dev-intchg-02-s3-scheme-fee` creado por infra (confirmado vía `aws s3 ls`, vacío).
- [x] Glue job `itl-0004-itx-dev-intchg-02-glue-scheme-fee` creado por infra con Role/Connections/WorkerType/GlueVersion/Timeout correctos.
- [x] `scheme_fee.py` subido a `s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/scheme_fee.py`.
- [x] `DefaultArguments` del job completados vía `aws glue update-job` (antes sólo tenía los genéricos de Glue — `--mode`, `--client_code`, `--report_month`, `--scheme_fee_bucket`, etc. no estaban, el job habría fallado al no encontrar esos argumentos).
- [x] `scripts/sync-glue.ps1`: agregada entrada `"scheme-fee"` → `glue\scripts\reports\scheme_fee`; corregida entrada `"test-2"` → `glue\scripts\reports\exchange_rates` (antes apuntaba por error a la carpeta de scheme_fee). Sync corrido para ambos jobs — `glue/scripts/reports/exchange_rates/` (nueva, `format_exchange_rates.py` + config/args) y `glue/scripts/reports/scheme_fee/` quedaron actualizados desde AWS.

**Aprovechado como referencia (2026-07-02, antes de desplegar):** descargado un CSV real de producción reciente del bucket legacy `itl-0004-itx-dev-s3-scheme-fee-02` (`OUT/SBSA/SBSA_202509_2026112_1768245876.csv`, generado 2026-01-12) — el header de 27 columnas coincide EXACTO (mismo orden, mismos nombres) con lo que genera `build_legacy_export()` en el script nuevo. El archivo `IN/SBSA/SBSA_202505_2025626_1750960566.csv` (round-trip de prueba, valores `txn_sfc=1000` uniformes — parece dummy/test) confirma que el archivo de vuelta NO necesita traer `trvl_prg_ind`/`grc_mpymt_ind`/`key_etrd_tpe` (se corta en `swt_cd`, 24 columnas) — consistente con que `run_read()` sólo lee `app_id`/`txn_cnt`/`txn_sfc`/`est_sch_fee_amt`.
- [x] **Subir 3 tablas de referencia nuevas a s3-reference — HECHO 2026-07-02.** El usuario subió los CSVs locales a `tst_files/reference_data/` (`bin_funding_source.csv`, `size_ticket.csv`, `scheme_fee_bin_products.csv` — esta tercera NO estaba en el plan original, ver nota abajo). Convertidos a Parquet (se eliminaron las columnas `app_creation_date`/`app_creation_user`, no usadas por el script) y subidos + verificados leyendo de vuelta desde S3: `s3://itl-0004-itx-dev-intchg-02-s3-reference/{bin_funding_source,size_ticket,scheme_fee_bin_products}/data.parquet` (5, 67 y 277 filas respectivamente, columnas coinciden con lo que esperan `load_bin_funding_source()`/`load_size_ticket()`/`load_scheme_fee_bin_products()`).
  - **Corrección importante de diseño (2026-07-02):** el plan original decía "agregar columna opcional `legacy_product_id` a `visa_bin_products`/`mastercard_bin_products`" (las tablas que ya usa `get_transaction.py`), asumiendo que eran la misma data que `m_scheme_fee_bin_products` del legacy. Al comparar los 3 archivos reales se encontró que **están desalineadas**: 2 `product_code` de VISA y 17 de Mastercard tienen `range_program_id` DISTINTO entre `visa_bin_products`/`mastercard_bin_products` y `scheme_fee_bin_products`, y `mastercard_bin_products` tiene 316 filas vs 224 de la versión scheme-fee (cobertura distinta). El legacy de scheme fee siempre usó `m_scheme_fee_bin_products` específicamente — usar las otras tablas habría dado `prg_id`/`prd_id` incorrecto para esos ~19 `product_code`. **Corregido en el script:** nueva función `load_scheme_fee_bin_products()` reemplaza a `load_visa_bin_products()`/`load_mastercard_bin_products()` para este job; `range_program_id` y `legacy_product_id` ahora salen AMBOS de `scheme_fee_bin_products` (antes sólo `legacy_product_id` era la parte "faltante"). Detalle en `scheme_fee_job_design.md`.
  - **Bug adicional corregido:** `load_size_ticket()` esperaba columna `size_ticket_id`, pero el CSV real usa `app_id` para eso — corregido (alias `app_id → size_ticket_id`).
  - **Ojo con los valores de `brand`:** `scheme_fee_bin_products` usa `'VISA'`/`'MC'`; `size_ticket` usa `'VISA'`/`'MasterCard'` — inconsistentes entre sí. El script ya maneja cada uno con el valor correcto por separado (no normalizar a un solo formato sin revisar ambos casos).
- [x] **Correr `--mode generate` end-to-end — SUCCEEDED 2026-07-03.** SBSA enero 2026 (`jr_799a63afc0d704fcb53c8d94b90f8c5a5c0a63414a4d6f6b1ae766b52b4d7130`, 1604s/~27min): 114,495,796 filas de detalle (34.8M Visa BASEII + 79.6M MC IPM_1240 + 702 MC IPM_1442, más duplicado on-us ya que SBSA tiene `dup_on_us_visa`/`dup_on_us_mc`=TRUE) → 434,014 filas de reporte. CSV confirmado en `s3://itl-0004-itx-dev-intchg-02-s3-scheme-fee/OUT/SBSA/SBSA_202601_202673_1783113206.csv` (53.2 MB). Estado interno confirmado en `s3://itl-0004-itx-dev-intchg-02-s3-analytics/SBSA/scheme_fee/state/202601/` (detail/ ~2.0 GB, report/ ~8.6 MB, summary.json consistente). Tardó 2 intentos previos fallidos — ver bugs corregidos abajo. **Pendiente:** revisar el CONTENIDO del CSV a mano (que "corrió sin error" no implica "los números son correctos") — EBGR (más liviano) puede servir para una segunda corrida de validación de contenido.
- [x] **2 bugs corregidos en la primera corrida real (2026-07-03), ver `scheme_fee_job_design.md` sección "Primera corrida real":** (1) `--in_file_key=""` en `DefaultArguments` rompía `getResolvedOptions` (Glue no maneja bien argumentos vacíos en `--arguments`/`DefaultArguments` — gotcha genérico de Glue a recordar) — cambiado a `"N/A"`. (2) `settlement_amount` no existe como columna real en el operational Visa (sólo existe para VSS, no BASEII) — cambiado a `NULL` explícito en `transform_visa_baseii_scheme_fee`/`transform_visa_sms_scheme_fee` (campo no usado aguas abajo). Script re-subido a S3 tras cada fix.
- [x] **Comparativo contra legacy real (2026-07-03) — bug encontrado, corregido Y VALIDADO.** El usuario subió el CSV legacy real de SBSA enero 2026 (`tst_files/scheme_fee_reports/sbsa/SBSA_202601_202626_1770395691.csv`, 460,094 filas) junto al CSV nuevo. Script de comparación: `tst_files/scheme_fee_reports/compare_scheme_fee_sbsa.py`. **Causa raíz:** `business_mode` se genera en mayúscula en `glue-vi-calculate` pero en minúscula en `glue-mc-calculate` — `scheme_fee.py` comparaba en mayúscula sin normalizar, rompiendo el duplicado on-us de MC y el mapeo `bus_id` del CSV final (100% de filas de MC caían en sentinel 255). Fix: `F.upper(F.col("business_mode"))`. **Resultado tras el fix (4to run, SUCCEEDED):** txn_cnt global de -4.09% a **+0.64%**, txn_amt de -4.99% a **-0.25%**; `bus_id=255` para MC pasó de 100% a 0%. Ver gotcha en `gotchas.md` y detalle completo en `scheme_fee_job_design.md`.
- [ ] **2 residuales menores identificados (no son bugs de `scheme_fee.py`, apuntan a `glue-mc-calculate`):** (1) `prd_id=1006` ("MDS") vs `1054` ("MDU") — legacy tiene todo en 1006, el nuevo lo divide entre ambos (suma similar, ~+781K txn_cnt de diff) — sugiere diferencia en el BIN-range lookup de `product_id`. (2) `fnd_src_id=116` (código "P", confirmado que SÍ existe en `bin_funding_source.parquet`) ausente al 100% en el nuevo — `funding_source` de MC nunca produce ese valor. Investigar en `glue/scripts/mastercard/calculate/calculate.py`, no en scheme_fee.py. Detalle en `scheme_fee_job_design.md`.
- [ ] **Probar el ciclo completo `--mode read`** con un CSV de vuelta simulado (rellenar `txn_sfc`/`est_sch_fee_amt` a mano) para validar el join por `app_id` y la propagación al detalle. El archivo dummy legacy `IN/SBSA/SBSA_202505_2025626_1750960566.csv` (ver arriba) puede servir de plantilla de formato, pero sus `app_id` no van a alinear con los que genere `--mode generate` del script nuevo (agrupación distinta) — no reusar directo, sólo como referencia de columnas.
- [ ] **Validar campos de Visa SMS** (`ENABLE_SMS=False` por defecto) contra un Parquet SMS real antes de activarlo — mismo estado pendiente que en `get_transaction.py`.
- [ ] **Simplificación aceptada:** `switch_code` siempre 0 (SBSA "local switch" no portado — requiere metadato de nombre de archivo original que no existe en esta arquitectura). Ver docstring del script.
- [x] Actualizar el rename pendiente de jobs (sección "Cleanup de recursos obsoletos" abajo) — 2026-07-03: `glue-test-2` mantiene su función de exchange rates, se renombra a `glue-exchange-rates` (no a algo relacionado con scheme fee).

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

Inventario detallado con orden de ejecución: `tst_files/ticket_cleanup_itx_dev.txt`. Re-verificado en vivo contra AWS 2026-07-02; ajustado 2026-07-03 tras conversación del usuario con el equipo (crawler-reference vuelve a la lista de eliminación, jobs a renombrar ajustados, agregado Glue job nuevo).

### AWS Glue

**Crawlers — eliminar:**
- `itl-0004-itx-dev-intchg-02-crawler-staging` — convención antigua (guiones), nunca ejecutado, apunta a todo `s3-staging/`. Verificado 2026-07-02, sigue igual.
- `itl-0004-itx-dev-intchg-02-crawler-reference` — **re-agregado 2026-07-03** tras decisión del equipo de consolidarlo con `itl_0004_itx_dev_02_glue_crawler_exchange_rates` (ya cubre la misma database con la convención de nombres correcta). Aunque está activo (schedule diario, corrió exitosamente el 2026-07-02, escribe en la database oficial `itl_0004_itx_dev_02_glue_database_exchange_rates`), el equipo decidió de todas formas eliminarlo. **⚠️ Advertencia pendiente de confirmar antes de ejecutar:** `crawler-reference` apunta a `s3-reference/currency/`, mientras que `itl_0004_itx_dev_02_glue_crawler_exchange_rates` apunta a `s3-reference/exchange-rates/` — son prefijos S3 DISTINTOS. Si se elimina sin agregar `currency/` como target adicional en otro crawler, se pierde la cobertura de catálogo sobre esa ruta (habilitada recién el 2026-07-01). Ver detalle completo del historial de este punto en `tst_files/ticket_cleanup_itx_dev.txt` sección 1.

**Databases — eliminar:**
- `itl_0004_itx_dev_poc_ebgr_visa_staging` — convención `poc_*`, 0 tablas (verificado 2026-07-02)
- `itl_0004_itx_dev_poc_interchange_analytics` — convención `poc_*`, 0 tablas (verificado 2026-07-02), sin crawler asociado
- `itl_0004_itx_dev_poc_itx_reference` — sus 4 tablas son restos huérfanos de la configuración ANTERIOR de `crawler-reference` (ver arriba), apuntan a `s3-reference/exchange-rates/` — la misma ruta que ya cubre el crawler oficial `itl_0004_itx_dev_02_glue_crawler_exchange_rates` (escribe en la database oficial, con sus propias 4 tablas frescas). Los datos en S3 están sanos y catalogados en el lugar correcto; sólo el catálogo duplicado en esta database quedó huérfano desde el 2026-07-01.

**Jobs — renombrar** (actualizar `$AllJobs` en `sync-glue.ps1` al renombrar):
- `itl-0004-itx-dev-intchg-02-glue-test-1` → `itl-0004-itx-dev-intchg-02-glue-get-transaction` (ajustado 2026-07-03, antes decía `glue-vi-mc-reporting`)
- `itl-0004-itx-dev-intchg-02-glue-test-2` → `itl-0004-itx-dev-intchg-02-glue-exchange-rates` (agregado 2026-07-03 — mantiene su función actual de exchange rates, NO se repurposea para scheme fee)
- `itl-0004-itx-dev-intchg-02-glue-test-3` → `itl-0004-itx-dev-intchg-02-glue-vi-data-quality`
- `itl-0004-itx-dev-intchg-02-glue-test-4` → `itl-0004-itx-dev-intchg-02-glue-mc-data-quality`

**Job nuevo — creado por infra 2026-07-03 ✓:**
- `itl-0004-itx-dev-intchg-02-glue-scheme-fee` — corre `glue/scripts/reports/scheme_fee/scheme_fee.py`. Script subido a S3 y `DefaultArguments` completados el mismo día (ver sección "Scheme Fee" arriba). Falta correr `--mode generate` end-to-end.

### AWS S3

**Bucket nuevo — creado por infra 2026-07-03 ✓:**
- `itl-0004-itx-dev-intchg-02-s3-scheme-fee` — bucket de intercambio OUT/IN con el equipo externo que calcula el costo del módulo Scheme Fee. Confirmado creado y vacío. **No confundir con `itl-0004-itx-dev-s3-scheme-fee-02`**, que es del sistema legacy (EC2) y sólo sirve como referencia de formato. Pendiente: confirmar que `itl-0004-itx-dev-intchg-02-glue-role` tenga permiso de lectura/escritura sobre él (no verificable sin `iam:GetRole`; se sabrá al primer `--mode generate`).

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
