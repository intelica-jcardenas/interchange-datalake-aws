# Pendientes del proyecto

Checklist vivo — solo tareas activas de **desarrollo de código/pipeline**
(no sincronización ni documentación de scripts — eso se trackea en la
memoria de usuario: ver `push_sync_scripts_design.md` e
`itx_document_script_skill.md`). Lo resuelto se documenta en
`decisions.md`/`gotchas.md` (o en la memoria de usuario correspondiente)
y se borra de aquí — no se acumulan items completados con `[x]`.
Última actualización: 2026-08-13.

---

## `lmbd-vi-transform`: TCs nuevos Visa (RETURNED/RECLASSIFICATION/BASEII extendido) — CERRADO 2026-08-11

Trabajo completo, validado a fondo con datos reales (cliente NXGR) y cerrado
a pedido del usuario. Detalle completo → `decisions.md`. El consumo
downstream (`extract`/`clean`/`calculate`/`interchange`/`visa_fields`) de
`RETURNED`/`RECLASSIFICATION`/columna `"D"` **no es un pendiente** — se
acordó explícitamente que esta etapa solo carga hasta `staging` vía
`transform`, sin extenderse más allá. Queda abierto solo lo que sigue:

- [ ] **Crawler/database de Glue para `NXGR`** — confirmado que no existe
  ninguno (ni para VISA ni MASTERCARD) — ya hay Parquets reales en
  `s3-staging/NXGR/VISA/...` pero nadie puede consultarlos vía Athena/
  catálogo. Ofrecido al usuario, decidido dejarlo para después.
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
- [ ] Crawlers nuevos para `s3-analytics` (hoy no existe ninguno) — sección
  5 de la propuesta, sin crear todavía. Es el paso que le da sentido
  práctico a toda la reestructuración (sin esto nada de esto es
  consultable desde Athena).
- [ ] `--mode read` de `scheme_fee.py` (con el `write_parquet_multi()`
  nuevo aplicado a `final/detail`/`final/report`) no se probó con datos
  reales — solo `--mode generate`. Sigue sin CSV real del equipo externo
  (ver pendiente de scheme_fee más abajo), así que la validación seguiría
  siendo con costos dummy si se hace.

---

## `glue-exchange-rates` (`format_exchange_rates.py`) — versión en AWS vs versión en repo, sin resolver

El sync completo del 2026-08-10 encontró que la versión desplegada en AWS
(fecha 2026-08-03) usa el sink nativo de Glue (`enableUpdateCatalog`,
`glueContext.purge_s3_path()`) en vez del registro manual de particiones
por `boto3` que tiene el repo hoy. El usuario sospecha que la de AWS podría
ser una versión **anterior/incorrecta** — descartado el cambio en el repo
por ahora (`git restore`), **sin tocar AWS**. Detalle completo de ambas
versiones → `decisions.md`.

- [ ] **Confirmar con el encargado de ese script** cuál versión es la
  correcta antes de decidir si se sincroniza o se hace push del repo hacia
  AWS.

---

## `glue-ebgr-report` (Eurobank Merchant Report) — documentado y desplegado 2026-08-13, ver `decisions.md`

**Contexto (encontrado sin documentar 2026-08-10, desarrollo confirmado terminado por el usuario 2026-08-13):** recrea el reporte CSV legacy de scheme fee/comercios (RPT_MCT), específico para EBGR, acotado a Mastercard. Documentado completo con la skill `itx-document-script` (50 funciones + módulo, `DOC-ONLY` verificado, sin cambios de lógica) y desplegado al `ScriptLocation` real — ver `decisions.md` para el detalle del flujo/reglas.

- [ ] **Sin validar contra legacy** — el job corre y escribe el CSV (output confirmado en `s3-analytics/EBGR/reports/ebgr_merchant/`), pero nunca se comparó su resultado contra el reporte legacy real (mismo patrón de validación ya usado para `get_transaction.py`/`scheme_fee.py`/etc. — no se hizo acá todavía).

---

## Automatización `lmbd-rules-refresh` (refresh de visa_rules/mc_rules) — probado 2026-08-10, primer refresh REAL de producción 2026-08-11, ver `decisions.md`

**2026-08-11:** primer uso real del trigger para publicar un cambio de negocio genuino (no solo smoke test) — fix de expansión de familias `TRANSACTION_CODE` (excel V38) desplegado a `lmbd-test-1` y disparado subiendo el excel a S3. Backup automático + publicación + smoke test contra EBGR/SBSA, cero regresión. Ver decisión "`visa_rules`: excel V38 simplifica `TRANSACTION_CODE`..." en `decisions.md`.

- [ ] **Rol IAM dedicado** — **2026-08-17: rename a Lambda definitivo YA
  RESUELTO** (infra creó `itl-0004-itx-dev-intchg-02-lmbd-rules-refresh`,
  desactivó el trigger S3 de `lmbd-test-1` y lo dejó apuntando solo al
  nuevo; código desplegado y validado end-to-end contra AWS real por esta
  sesión — ver `decisions.md`). Sigue pendiente solo el rol IAM propio —
  el Lambda nuevo quedó sobre el mismo rol compartido `lmbd-vi-role`, sin
  permisos dedicados.
- [ ] **`mc_data_quality.py` corrió por primera vez con datos reales**
  (2026-08-10, EBGR/2026-01-05) pero solo como smoke test del fix de
  paths — sigue sin ninguna validación de que sus *resultados* (filas de
  validación, discrepancias detectadas) sean correctos contra legacy.
  Pendiente si se decide integrar este job al pipeline real.

**Nota:** se evaluó y descartó explícitamente una tabla DynamoDB de
auditoría (`rules_control`) para este Lambda — proceso chico y de baja
frecuencia, no la justifica. El schema propuesto y toda referencia en
código/README fueron eliminados (no solo deshabilitados) — no queda
ningún rastro en el repo, ver `decisions.md`.

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

## Infraestructura AWS

- [ ] **Rol IAM `itx-lambda-extract-role`** — `lmbd-vi-extract` comparte
  rol del router. Crear rol propio con permisos mínimos.
- [ ] **Rol IAM `itx-glue-crawler-ebgr-role`** — crawler Mastercard sin
  rol propio.
- [ ] **Agregar `s3-reference/currency/` como target** a un crawler
  existente o crear uno dedicado — baja prioridad.

---

## Housekeeping tst_files/ y S3 (bajo impacto, no urgente)

- [ ] **`glue-exchange-rates` (`format_exchange_rates.py`) y
  `glue-vi-data-quality` (`vi_data_quality.py`) siguen expuestos** —
  ambos escriben con el writer nativo de Spark
  (`.write.mode(...).parquet(path)`, el segundo con `partitionBy` en
  exchange-rates) sin pasar por `write_single_parquet()`/
  `write_parquet_multi()`. Confirmado 2026-08-13: 2 marcadores ya
  existentes en `s3-reference/exchange-rates-glue/brand={Mastercard,Visa}`
  (sin tocar, se irían regenerando). Deliberadamente fuera de alcance
  de esta sesión — `vi-data-quality` ni siquiera está integrado a un
  Step Function todavía (bajo impacto real); `exchange-rates` sigue con
  su propio pendiente sin resolver (versión AWS vs repo, ver sección
  propia más arriba) — no tiene sentido tocar su writer hasta resolver
  eso primero.

---

## Ambiente empresarial

- [ ] **Testing end-to-end en ambiente empresarial** — pendiente cuando
  el ambiente esté disponible.
