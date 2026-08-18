# Pendientes del proyecto

Checklist vivo — solo tareas activas de **desarrollo de código/pipeline**
(no sincronización ni documentación de scripts — eso se trackea en la
memoria de usuario: ver `push_sync_scripts_design.md` e
`itx_document_script_skill.md`). Lo resuelto se documenta en
`decisions.md`/`gotchas.md` (o en la memoria de usuario correspondiente)
y se borra de aquí — no se acumulan items completados con `[x]`.

**Alcance (definido 2026-08-17):** este checklist trackea solo trabajo que
bloquea o afecta el desarrollo del pipeline (código, validaciones,
correctitud de datos). Provisión de infraestructura AWS (roles IAM
dedicados, creación/rename de Lambdas) es responsabilidad del equipo de
Infra — se coordina por ticket aparte y no se trackea acá salvo que
bloquee algo nuestro. Housekeeping/cleanup cosmético (marcadores
`_$folder$`, crawlers de solo-catálogo sin desarrollo downstream
esperándolos) tampoco se trackea acá salvo que cause un error real. Ambos
tipos de items, cuando existen, viven en una nota compacta al final de
este archivo — no como checklist activo.

Última actualización: 2026-08-17.

---

## `lmbd-vi-transform`: TCs nuevos Visa (RETURNED/RECLASSIFICATION/BASEII extendido) — CERRADO 2026-08-11

Trabajo completo, validado a fondo con datos reales (cliente NXGR) y cerrado
a pedido del usuario. Detalle completo → `decisions.md`. El consumo
downstream (`extract`/`clean`/`calculate`/`interchange`/`visa_fields`) de
`RETURNED`/`RECLASSIFICATION`/columna `"D"` **no es un pendiente** — se
acordó explícitamente que esta etapa solo carga hasta `staging` vía
`transform`, sin extenderse más allá. Queda abierto solo lo que sigue:

- [ ] **3 combinaciones TC/TCSN sin probar con datos reales** (solo tests
  sintéticos): TC `15/16/17/35/36/37` y TCSN `"D"` en BASEII (los 7
  records BASEII reales vistos eran todos del rango viejo 05-27), y TC
  `03` (Returned Nonfinancial) — solo se vio `01`/`02` en datos reales.
  Riesgo bajo (mismo `_tcsn_ordinal`/mecanismo de sets ya probado con
  datos reales para `D`/`E` en RETURNED/RECLASSIFICATION) — repetir el
  chequeo de tabulación TC/TCSN si aparece un archivo real que los use,
  sin necesidad de buscarlos activamente.
- [ ] **EBGR/SBSA — riesgo aceptado, sin acción:** si algún día reciben
  archivos con los TC nuevos, la columna `"D"` puede generar un
  `HIVE_BAD_DATA`/`HIVE_PARTITION_SCHEMA_MISMATCH` transitorio en Athena
  hasta re-correr el crawler (`staging_ebgr_visa`/`staging_sbsa_visa`, sin
  schedule, no se autocorrigen). Mismo fix ya validado varias veces en
  `gotchas.md` — decidido no hacer nada proactivo, solo re-crawlear si
  pasa.

(El crawler/database de Glue para `NXGR` — habilita consultas Athena, sin
downstream de `extract`/`clean`/`calculate` esperándolo todavía — se movió
a la nota de items fuera de alcance al final de este archivo.)

---

## Estructura Hive-partitioned para reportes en `s3-analytics` — DESPLEGADO Y VALIDADO CONTRA AWS REAL 2026-08-12

Propuesta completa en artifact: https://claude.ai/code/artifact/662380d0-15b7-42c4-8c0b-42e6c97b1403.
Implementada 2026-08-11, revisada y corregida (rediseño de `adhoc_tag`) el
2026-08-12, desplegada y validada con datos reales el mismo día — ver
`decisions.md` para el detalle completo (diseño final, validación DuckDB de
ambos scripts, hallazgo del `app_id` no-determinístico en `scheme_fee`).

**Código final (desplegado):**
- `get_transaction.py`: `report_suffix` eliminado. 2 args: `report_month`
  (obligatorio, YYYYMM) y `adhoc_tag` (vacío = oficial, sobreescribe
  `report_month={report_month}/data.parquet`; no vacío = adhoc, su propio
  valor nombra `_adhoc/{adhoc_tag}/data.parquet`). `write_single_parquet()`
  nueva — evita el marcador `_$folder$` (mismo patrón que
  `mastercard/interchange.py`).
- `scheme_fee.py`: `STATE_PREFIX`/`FINAL_PREFIX` con `report_month=`.
  `write_parquet_multi()` nueva — variante multi-archivo de lo anterior (no
  fuerza `coalesce(1)`, preserva paralelismo en datasets de varios GB).
  `in_file_key` dejó de ser obligatorio en Glue para `--mode generate`
  (antes rompía con `GlueArgumentError` si se le pasaba `""`).
- CSV `IN/`/`OUT/` en `s3-scheme-fee` — confirmado sin tocar en ningún punto.

**Datos existentes en `s3-analytics` ya reorganizados y verificados (copia
server-side + verificación de tamaño byte a byte antes de borrar el original,
originales borrados recién después de confirmar):**
- EBGR: único reporte existente (`ebgr_202601`) → promovido a oficial,
  `EBGR/reports/get_transaction/report_month=202601/data.parquet`.
- SBSA: 5 reportes existentes → **`sbsa_202601_hallazgo5bfix` (2026-07-28)
  promovido a oficial** (el más reciente Y semánticamente el último estado
  validado de VI tras el fix de Hallazgo 5b — ver `decisions.md`; confirmado
  que el fix de MC token_flag del 2026-07-30, posterior, no afecta este pick
  porque no tuvo ningún match real en tráfico SBSA). Los otros 4
  (`sbsa_202601`, `sbsa_202601_byfix`, `sbsa_202601_sms_test`,
  `202601_scheme_fee_union_test`) → `SBSA/reports/get_transaction/_adhoc/{nombre}/data.parquet`.
- `scheme_fee`: `EBGR/scheme_fee/state/202601/` → `state/report_month=202601/`;
  `SBSA/scheme_fee/{state,final}/202601/` → `{state,final}/report_month=202601/`
  (incluye limpieza de los marcadores `_$folder$` de esas 2 carpetas).
- CSV `IN/`/`OUT/` en `s3-scheme-fee` — no tocado, fuera del alcance.
- `EBGR/reports/ebgr_merchant/`, `EBGR/reports/quality/` (otros jobs,
  `ebgr_merchant.py`/`mc_data_quality.py`) — confirmado que NO son
  `get_transaction`, no se tocaron.

**Pendiente real:**
- [ ] `--mode read` de `scheme_fee.py` (con el `write_parquet_multi()`
  nuevo aplicado a `final/detail`/`final/report`) no se probó con datos
  reales — solo `--mode generate`. Sigue sin CSV real del equipo externo
  (ver pendiente de scheme_fee más abajo), así que la validación seguiría
  siendo con costos dummy si se hace.

(Los crawlers nuevos para `s3-analytics` — habilitan Athena, no bloquean
ningún desarrollo de script — se movieron a la nota final.)

---

## `glue-ebgr-report` (Eurobank Merchant Report) — documentado y desplegado 2026-08-13, ver `decisions.md`

**Contexto (encontrado sin documentar 2026-08-10, desarrollo confirmado terminado por el usuario 2026-08-13):** recrea el reporte CSV legacy de scheme fee/comercios (RPT_MCT), específico para EBGR, acotado a Mastercard. Documentado completo con la skill `itx-document-script` (50 funciones + módulo, `DOC-ONLY` verificado, sin cambios de lógica) y desplegado al `ScriptLocation` real — ver `decisions.md` para el detalle del flujo/reglas.

- [ ] **Sin validar contra legacy** — el job corre y escribe el CSV (output confirmado en `s3-analytics/EBGR/reports/ebgr_merchant/`), pero nunca se comparó su resultado contra el reporte legacy real (mismo patrón de validación ya usado para `get_transaction.py`/`scheme_fee.py`/etc. — no se hizo acá todavía).

---

## Automatización `lmbd-rules-refresh` (refresh de visa_rules/mc_rules) — EN PRODUCCIÓN sobre el Lambda definitivo desde 2026-08-17, ver `decisions.md`

**2026-08-11:** primer uso real del trigger para publicar un cambio de negocio genuino (no solo smoke test) — fix de expansión de familias `TRANSACTION_CODE` (excel V38). Backup automático + publicación + smoke test contra EBGR/SBSA, cero regresión. Ver decisión "`visa_rules`: excel V38 simplifica `TRANSACTION_CODE`..." en `decisions.md`.

**2026-08-17:** migrado de `lmbd-test-1` (infra prestada) al Lambda definitivo `itl-0004-itx-dev-intchg-02-lmbd-rules-refresh` (creado por Infra) — código desplegado y validado end-to-end contra AWS real (mismo excel V38 ya archivado, diff=0). Ver decisión "`lmbd-rules-refresh`: migración de `lmbd-test-1`..." en `decisions.md`.

**Nota:** se evaluó y descartó explícitamente una tabla DynamoDB de
auditoría (`rules_control`) para este Lambda — proceso chico y de baja
frecuencia, no la justifica. El schema propuesto y toda referencia en
código/README fueron eliminados (no solo deshabilitados) — no queda
ningún rastro en el repo, ver `decisions.md`.

(El rol IAM dedicado para este Lambda — sigue sobre el rol compartido
`lmbd-vi-role` — es gestión de Infra, ver nota final.)

---

## `glue-mc-data-quality` — sin validar resultados contra legacy, ver `decisions.md`

Corrió por primera vez con datos reales el 2026-08-10 (EBGR/2026-01-05, smoke
test del fix de paths). El 2026-08-17 se sincronizó un rewrite sustancial
del script (fixes reales de precisión decimal, `hash_file_filter` de SBSA,
MTI 1442, overrides de `validation_conditions`) hecho por el encargado real
del job entre el 2026-07-30 y el 2026-08-12 — aceptado tal cual, más la
limpieza de `DefaultArguments` (recurrencia del mismo problema ya resuelto
una vez, ver decisión "Estandarización de configuración de Glue Jobs").

- [ ] **Sin ninguna validación de que los *resultados* (filas de validación,
  discrepancias detectadas) sean correctos contra legacy** — ni antes ni
  después del rewrite del 2026-08-17. Pendiente si se decide integrar este
  job al pipeline real.

---

## Configuración `vi-data-quality` pendiente de confirmar

(Nota: los pendientes de bajo impacto de reglas Visa/MC —SBSA VI sin
comparativo post-V37, `intelica_id` NULL en MC, ATM JPY rule 1055/1065,
SMS +60.55%— se sacaron del checklist activo 2026-08-13 a pedido del
usuario, por no justificar el esfuerzo frente al impacto. Siguen con su
detalle técnico completo en `gotchas.md`/`decisions.md`, solo dejan de
trackearse acá.)

- [ ] **`vi-data-quality`**: `NumberOfWorkers=10` vs sus pares de
  reportería (`2`) — sigue sin confirmar, dejado sin tocar a pedido
  explícito del usuario. `MaxConcurrentRuns` cambió de `1` a `50` en AWS
  el 2026-07-31 (fuera de esta sesión, no lo tocamos nosotros) —
  confirmado con un sync completo de los 9 Glue jobs (los otros 8 sin
  ningún diff, script+config+args idénticos a AWS). Repo local
  actualizado para reflejar el `50` real. Sin decidir si es intencional
  (alineación con sus pares, que ya usan 50) o el mismo patrón de
  "contaminación de consola" ya visto en otros jobs — ver
  `decisions.md` → "Estandarización de configuración de Glue Jobs".

---

## Fee MC en moneda (`calculate_mastercard_fee_pyspark` rewrite) — desplegado, validación parcial

Ver memoria de usuario `mc_interchange_fee_currency_rewrite.md` para el
detalle completo. Ya desplegado y commiteado; validado para SBSA
(2026-07-17: `interchange_fees_amount` -0.00056% vs legacy). Pendiente:

- [ ] EBGR sin re-validar con este fix específico (bajo valor esperado
  — EBGR es abrumadoramente IN, y el fix solo afecta OUT).
- [ ] `vi_data_quality.py`/`mc_data_quality.py` — fix de moneda
  desplegado y commiteado (2026-07-20) pero nunca vuelto a ejecutar con
  datos reales desde entonces.

---

## Ambiente empresarial

- [ ] **Testing end-to-end en ambiente empresarial** — bloqueado por
  disponibilidad del ambiente (provisión de Infra, fuera de nuestro
  control). Retomar cuando el ambiente exista.

---

## Fuera de alcance de este checklist (gestión de Infra / housekeeping cosmético)

Referencia, no checklist activo — nada de esto bloquea el desarrollo del pipeline:

- **Roles IAM dedicados** — `lmbd-vi-extract` (comparte rol con el router),
  `lmbd-rules-refresh` (comparte `lmbd-vi-role`, ver arriba), crawler MC
  EBGR (`itx-glue-crawler-ebgr-role`, sin rol propio). Provisión de Infra,
  se coordina por ticket — mismo patrón que la creación/rename de
  `lmbd-rules-refresh` (ver `decisions.md`).
- **Crawlers/databases de Glue para catálogo Athena** — `NXGR` (VISA/MC,
  sin ninguno hoy), `s3-analytics` (reportes reorganizados, ver arriba),
  `s3-reference/currency/` como target de un crawler existente. Habilitan
  consultas ad-hoc, no bloquean ningún desarrollo de script en curso.
- **Housekeeping de writers Spark sin migrar** — `glue-exchange-rates`
  (`format_exchange_rates.py`) y `glue-vi-data-quality`
  (`vi_data_quality.py`) siguen generando el marcador `_$folder$` (0
  bytes, cosmético) porque no pasaron por el fix de `write_single_parquet()`/
  `write_parquet_multi()` ya aplicado al resto del pipeline. Sin impacto
  funcional confirmado — deliberadamente sin tocar hasta que
  `glue-exchange-rates` resuelva su pendiente de versión (arriba) y
  `vi-data-quality` se integre a algún Step Function.
- **Inventario de migración DEV→PRD (Terraform, Infra)** — revisado
  2026-08-18 contra AWS real, 2 gaps críticos encontrados (catálogo de
  Glue subestimado 4 vs 18 recursos reales; buckets S3 con mapeo dudoso,
  recurso llamado `aws_s3_bucket.poc`) más 2 importantes (falta
  `lmbd-archive-file`; `for_each` de Lambdas no distingue Visa/Mastercard).
  Coordinación con Infra, no bloquea desarrollo del pipeline — detalle
  completo en `decisions.md`.
