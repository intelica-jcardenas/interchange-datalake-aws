# Pendientes del proyecto

Checklist vivo — solo tareas activas de **desarrollo de código/pipeline**
(no sincronización ni documentación de scripts — eso se trackea en la
memoria de usuario: ver `push_sync_scripts_design.md` e
`itx_document_script_skill.md`). Lo resuelto se documenta en
`decisions.md`/`gotchas.md` (o en la memoria de usuario correspondiente)
y se borra de aquí — no se acumulan items completados con `[x]`.
Última actualización: 2026-08-11.

---

## `lmbd-vi-transform`: TCs nuevos Visa (RETURNED/RECLASSIFICATION/BASEII extendido) — CERRADO 2026-08-11

Trabajo completo, validado a fondo con datos reales (cliente NXGR) y cerrado
a pedido del usuario. Detalle completo → `decisions.md`. Queda abierto solo
lo que sigue:

- [ ] **Downstream fuera de alcance a propósito:** `lmbd-vi-extract`/
  `lmbd-vi-clean`/`glue-vi-calculate`/`glue-vi-interchange` y la tabla
  DynamoDB `visa_fields` no saben nada todavía de `RETURNED`/
  `RECLASSIFICATION` ni de la columna `"D"` nueva en BASEII — esos Parquets
  se generan en `s3-staging` pero nada los consume aún más adelante en el
  pipeline. Definir cuándo se aborda (sesión futura).
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

## Estructura Hive-partitioned para reportes en `s3-analytics` — propuesta lista, sin implementar

Propuesta completa en artifact: https://claude.ai/code/artifact/662380d0-15b7-42c4-8c0b-42e6c97b1403
(`get_transaction.py` → `report_month=YYYYMM/data.parquet` + `_adhoc/{tag}/` para corridas
de investigación; `scheme_fee.py` → agregar `report_month=` a `state/`/`final/`, CSV
IN/OUT sin tocar). Motivada porque hoy no existe ningún crawler/database de Glue
apuntando a `s3-analytics` — no se puede consultar desde Athena vía catálogo todavía.

- [ ] **Revisar la propuesta y decidir si se implementa** — sin apuro, el usuario
  pidió dejarla anotada para revisar más adelante.
- [ ] Si se aprueba: decidir qué hacer con el histórico ya generado de
  `get_transaction` (reescribir vs. aplicar la estructura solo hacia adelante),
  y si `_adhoc/` vive en `s3-analytics` o en otro bucket.

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

## `glue-ebgr-report` (Eurobank Merchant Report) — encontrado sin documentar 2026-08-10, ver `decisions.md`

**Contexto confirmado por el usuario (2026-08-10):** recrea en el datalake
un reporte CSV que existe en legacy, específico para EBGR. Lo está
armando un miembro del equipo, en proceso de validación — **no tocar ni
investigar la lógica todavía**. Por ahora solo debe existir como entrada
mapeada en `sync-glue.ps1`/`push-glue.ps1` (ya hecho). Cuando el usuario
confirme que está terminado/validado, documentar con la skill
`itx-document-script` (mismo tratamiento que el resto de `glue/scripts/`).

- [ ] **Bucket `s3-analytics`** — usado por `get_transaction.py`/`scheme_fee.py`
  pero no aparece en la tabla de "S3 — 5 buckets" de `CLAUDE.md` — gap de
  documentación preexistente, no de hoy, notado de paso durante esta auditoría.

---

## Automatización `lmbd-rules-refresh` (refresh de visa_rules/mc_rules) — probado 2026-08-10, ver `decisions.md`

- [ ] **Layer con `openpyxl` y rol IAM dedicados** — hoy corre sobre
  infraestructura prestada (`lmbd-test-1`, rol `lmbd-vi-role`), sin
  Lambda ni rol propios.
- [ ] **Decidir si se queda sobre `lmbd-test-1` o se migra a un Lambda
  propio** antes de dejar el trigger S3→Lambda activo de forma
  permanente — hoy está activo y auto-publica sin aprobación manual
  (diseño ya acordado), pero sigue corriendo sobre infra prestada.
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

## Reglas Visa/MC — pendientes de bajo impacto

- [ ] **SBSA VI** — el reproceso masivo de 156 archivos (Hallazgo 5b) ya
  corre con los 3 fixes de V37 (`token_requestor_id`/`settlement_flag`/
  `cashback`) en el código, pero nunca se corrió un comparativo dedicado
  post-V37 para SBSA (solo se validó el fix de ARDEF). Bajo valor, sin
  decidir si vale la pena repetir el ejercicio hecho hoy para EBGR.
- [ ] **`intelica_id`/`region_country_code` NULL en MC (82.6% de EBGR
  IN, ~similar en SBSA)** — causa raíz NO encontrada (investigado a
  fondo 2026-07-30, ver `decisions.md`): confirmado que el motor de
  reglas de `interchange.py` (Spark) falla en matchear reglas activas
  bien formadas, por una razón no identificada en la ejecución de Spark
  (no es contenido de reglas, no es cálculo de jurisdicción, no es un
  problema de join/duplicados). **Confirmado que no afecta ningún $
  reportado hoy** (IN usa `amounts_transaction_fee_7_pds_146_7`, no
  `calculated_value`) — prioridad media, no bloquea producción.
  Instrumentar `assign_rules_simple()`/`prefilter_rules_needed()` con
  logging real y correrlo aislado sería el siguiente paso si se retoma.
- [ ] **ATM JPY rule 1055 vs 1065 (EBGR, 1 transacción)** —
  `glue-vi-interchange` asigna la regla equivocada, fee_fixed=0.50 USD
  faltante. Investigar campo diferenciador en `visa_rules`.
- [ ] **`interchange_fees_amount` de SMS +60.55% de más** (SBSA),
  concentrado 100% en `transaction_type_id=22` (ATM cash withdrawal) —
  bug en `glue-vi-interchange` (asignación de fee), no en
  `get_transaction.py`. Detalle → `gotchas.md`.
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

- [ ] **Carpetas `_$folder$` en Visa (staging/reference)** — marcador 0
  bytes que el committer nativo de Hadoop/Spark crea por cada partición
  nueva al escribir con `df.coalesce(1).write.parquet(path)`
  (`visa/calculate.py`/`visa/interchange.py`). Contados 2026-07-31:
  360 en `s3-staging` (casi todos `EBGR/VISA/400_baseii_cal_drafts/`,
  `500_baseii_itx_drafts/`), 2 en `s3-reference`
  (`exchange-rates-glue/brand={Mastercard,Visa}`), 0 en
  `s3-operational`. Cosmético — no rompe nada (Athena/Glue los
  ignoran), pero se regeneran cada corrida mientras el código siga
  usando ese writer; borrar los existentes no sirve sin cambiar el
  mecanismo de escritura. El patrón `write_single_parquet()` de
  `mastercard/interchange.py` (writer nativo a un prefijo temporal +
  `copy_object` del part-file al nombre final, sin pasar por el
  committer que genera el marcador) lo resolvería de raíz si se aplica
  a los 2 scripts de Visa — evaluado y descartado por ahora (esfuerzo
  real de código+deploy+prueba vs. beneficio puramente cosmético).

---

## Ambiente empresarial

- [ ] **Testing end-to-end en ambiente empresarial** — pendiente cuando
  el ambiente esté disponible.
