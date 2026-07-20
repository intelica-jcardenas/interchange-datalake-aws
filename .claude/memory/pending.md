# Pendientes del proyecto

Checklist vivo — solo tareas activas de **desarrollo de código/pipeline**
(no sincronización ni documentación de scripts — eso se trackea en la
memoria de usuario: ver `push_sync_scripts_design.md` e
`itx_document_script_skill.md`). Lo resuelto se documenta en
`decisions.md`/`gotchas.md` (o en la memoria de usuario correspondiente)
y se borra de aquí — no se acumulan items completados con `[x]`.
Última actualización: 2026-07-11.

---

## Pipeline Visa — residuales conocidos (bajos, aceptados)

- [ ] **ATM NO AF + ATM DCC NO AF (SBSA):** ~+63.6K ZAR de 179.7M
  (0.035%), count_legacy=0 en ambos casos. Investigar qué regla asignaba
  legacy. Sin revisar aún.
- [ ] **ATM JPY rule 1055 vs 1065 (EBGR, 1 transacción):**
  `glue-vi-interchange` asigna la regla equivocada, fee_fixed=0.50 USD
  faltante. Investigar campo diferenciador en `visa_rules`.

---

## Pipeline Mastercard — residual conocido (bajo, aceptado por ahora)

- [ ] **jurisdiction_code NULL-vs-off-us (SBSA, MC):** `off-us` con
  +6,812 de más vs legacy (nuestro LEFT JOIN contra IAR clasifica
  transacciones que el INNER JOIN de legacy deja sin clasificar). ~11%
  del gap explicado por el envelope de 9 dígitos del PAN en
  `calculate_pre2()`; ~89% restante (~6,000 transacciones) sin causa
  identificada. Impacto en dólares mínimo (+0.0045%). Detalle completo →
  `.claude/memory/gotchas.md`. Investigación pausada por bajo impacto.

---

## Pipeline Mastercard — cambios desplegados sin validar

**Diff revisado 2026-07-20** (`git diff` de los 8 archivos con cambios sin commitear): los 4 scripts de código (`interchange.py`, `get_transaction.py`, `mc_data_quality.py`, `vi_data_quality.py`) coinciden exacto con lo documentado abajo — sin código de debug, sin cambios inesperados. Los 4 compilan limpio (`py_compile`). Los 2 `args.json` modificados son solo cambios de `TempDir` (reorganización de paths en AWS vía sync, trivial). `gotchas.md`/`pending.md` son solo inserciones de documentación. Listo para commit — pendiente de que el usuario lo haga.

- [ ] **glue-mc-interchange — rewrite de `calculate_mastercard_fee_pyspark`
  (moneda del fee) — YA COMMITEADO Y DESPLEGADO, verificado 2026-07-14**
  (corrección: se creía "cambio local sin commitear", no lo era). El
  rewrite (`calculated_fee` siempre en `trx_ccy`/DE_49, en 1 paso) está
  commiteado desde `0d9ae133` ("Update 20260705 - Scheme Fee 1st
  Version", 2026-07-05/06) y confirmado desplegado en AWS (`aws s3api
  head-object` sobre el script en `s3-reference`: `LastModified
  2026-07-11`, mismo día del rollout de `push-glue.ps1`). Sigue
  pendiente lo real: (1) no hay ningún comparativo contra legacy
  corrido DESPUÉS de este cambio — el último comparativo MC (2026-06-30,
  +10,001 ZAR/+0.0045%) es anterior al rewrite y valida la lógica vieja
  de 2 pasos, no esta; (2) columna `settlement_currency_u` (línea ~1274)
  queda calculada y confirmada sin ningún uso posterior — candidata a
  limpieza; (3) sin revisar si esto reabre/explica los residuales ATM de
  arriba. Detalle completo → memoria de usuario
  `mc_interchange_fee_currency_rewrite.md` (desactualizada en el punto
  de "sin commitear", el resto sigue vigente).
- [ ] **get_transaction.py / mc_data_quality.py — fix de moneda del fee
  MC aplicado localmente (2026-07-14), sin desplegar ni validar.**
  Ambos asumían `calculated_value` en `rate_currency` (comportamiento
  viejo); ya se corrigieron para usar `trx_ccy`/DE_49 (decisión tomada
  con el usuario: mantener el rewrite de interchange y actualizar los
  reportes, en vez de revertir el rewrite). **Ojo:** el SP legacy real
  (`sql/get_mastercard_transactions.sql`) sí usa `rate_currency` — este
  fix hace que `interchange_fees_amount`/validación MC OUT YA NO
  coincida con legacy en transacciones cross-currency (antes sí
  coincidía). Falta: (1) correr comparativo real contra legacy para
  cuantificar ese residual nuevo y aceptado; (2) subir ambos scripts a
  S3 vía `push-glue.ps1`; (3) commitear (lo hace el usuario). Detalle
  completo → `.claude/memory/gotchas.md`.
- [ ] **vi_data_quality.py — mismo fix de moneda aplicado a Visa
  (2026-07-14), sin desplegar ni validar.** `itx_amt` (BASEII) usaba
  `interchange_fee_currency` para convertir el fee, cuando
  `interchange_fee_amount_itx` ya está en `source_currency` desde
  siempre (no hubo rewrite en Visa, el script nunca se alineó con esa
  convención). Corregido a usar solo `X1`/`xr1_rate` (misma tasa que
  `trx_amt`), eliminado el join `X2` redundante. Riesgo bajo — el job
  solo tiene un smoke test, nunca se validó `itx_amt` a fondo contra
  legacy. Falta: subir a S3, correr con datos reales y comparar
  antes/después. Detalle completo → `.claude/memory/gotchas.md`.
- [ ] **Plan de despliegue EJECUTADO para SBSA (2026-07-15/16) — falta
  correr la comparación y EBGR queda pendiente.** Los 4 scripts
  (`interchange.py`, `get_transaction.py`, `mc_data_quality.py`,
  `vi_data_quality.py`) confirmados subidos a S3 (`head-object`:
  `interchange.py` 2026-07-16T15:52, los otros 3 el 2026-07-15T19:31).
  `glue-mc-interchange` reprocesado para SBSA enero 2026 el 2026-07-16
  ~10:42 (104/104 SUCCEEDED). `glue-get-transaction` corrido con los
  cambios ya desplegados: `jr_0a90f54d...` (SBSA, `report_suffix=
  sbsa_202601`, `--scheme_fee false`, 2026-07-16 16:24–16:40,
  SUCCEEDED) → parquet en `s3-analytics/SBSA/reports/
  report_transactions_SBSA_sbsa_202601.parquet/` (3.0 GiB). Antes de
  correr ese job se respaldó el reporte SBSA anterior (pre-fix, del
  2026-07-09) como `tst_files/reporting/sbsa/
  report_transactions_SBSA_sbsa_202601_BEFORE_feefix.parquet`. El nuevo
  se descargó el 2026-07-17 como `..._AFTER_feefix.parquet`.
  **A/B interno (2026-07-17, `compare_before_after_feefix.py`, lógica
  vieja vs nueva mismo dataset):** VI sin diferencia; MC
  `interchange_fees_amount` -11,733.14 ZAR/-0.0051%, concentrado en
  `jurisdiction_code=intraregional`, `nulls_fee` sin cambio.
  **Validación contra legacy (2026-07-17, `compare_sbsa_after_feefix.py`
  — copia de `compare_sbsa.py` apuntando al parquet post-fix, cache
  legacy separada `legacy_acc_cache_sin_cuotas.pkl`):** MC
  `interchange_fees_amount` **-1,252.82 ZAR de 221.9M (-0.00056%)** —
  mejora clara vs. el baseline 2026-06-30 (+10,001 ZAR/+0.0045% con la
  lógica vieja), cambió de signo y se achicó ~8x. VI sin cambio
  (+68,285/+0.038%, residual ya conocido ATM NO AF/DCC). El gap
  `jurisdiction_code` NULL-vs-off-us de MC confirmado sin cambio (no lo
  toca el fix de moneda, es problema de join en `calculate_pre2()`).
  `scheme_fees_amount` 0=0 en ambos lados (columna existe en la tabla
  legacy pero sin poblar). **EBGR sigue sin reprocesar/re-comparar**
  (todo esto fue solo para SBSA) — pero confirmado (2026-07-17, código +
  datos S3) que es de bajo valor esperado: `get_transaction.py` solo lee
  `calculated_value` (el campo que cambia con este fix) para archivos
  **OUT**; los **IN** usan `amounts_transaction_fee_7_pds_146_7`, un
  campo no afectado. EBGR es abrumadoramente IN — `s3-operational/EBGR/
  MC/IPM_1240/`: 132 archivos IN (5.05 GB) vs 21 archivos OUT (2.66 MB
  total, ~0.05% del volumen), aunque sí hay OUT en 17 fechas de enero
  2026 (no faltan por completo). El residual esperado de repetir el
  ciclo en EBGR sería ínfimo — de baja prioridad, no descartado del
  todo. Sigue sin decidirse si esto reabre los residuales ATM de
  `pending.md`. Detalle completo → memoria de usuario
  `mc_interchange_fee_currency_rewrite.md`.

---

## Reporting (`get_transaction.py` / `scheme_fee.py`)

- [ ] **Validar `scheme_fees_amount` con costos REALES** (no dummy) —
  pendiente de un ciclo real `--mode read` con el CSV devuelto por el
  equipo externo de cuotas. Diseño y validación estructural ya cerrados
  (ver `project_status.md` / memoria de usuario `scheme_fee_job_design.md`).
- [ ] **Reactivar SMS en `process_client_range()`** — comentado a
  propósito (2026-07-09) mientras se completaba la unificación con
  scheme_fee; el fix ya está en el código, solo falta descomentar y
  re-validar la unión BASEII+SMS+MC juntos (la última corrida con los 3
  frames activos se canceló antes de confirmar que no rompe nada).
- [ ] **`interchange_fees_amount` de SMS +60.55% de más**, concentrado
  100% en `transaction_type_id=22` (ATM cash withdrawal) — bug en
  `glue-vi-interchange` (asignación de fee), no en `get_transaction.py`.
  Detalle completo → `.claude/memory/gotchas.md`. No bloquea el resto del
  reporting.
- [ ] **Retirar `.claude/memory/scheme_fee_generate_read_pipeline.md`**
  (marcado como temporal) una vez que Scheme Fee se dé por cerrado (ver
  pendiente de costos reales arriba) — fusionar lo que siga siendo
  relevante a `decisions.md`/`gotchas.md`.

---

## Infraestructura AWS

- [ ] **Rol IAM `itx-lambda-extract-role`** — `lmbd-vi-extract` comparte
  rol del router. Crear rol propio con permisos mínimos.
- [ ] **Rol IAM `itx-glue-crawler-ebgr-role`** — crawler Mastercard sin
  rol propio.
- [ ] **Agregar `s3-reference/currency/` como target** a un crawler
  existente (ej. `itl_0004_itx_dev_02_glue_crawler_exchange_rates`) o
  crear uno dedicado — baja prioridad.

---

## Housekeeping S3 (bajo impacto, no urgente)

- [ ] **Carpetas `_$folder$` en Visa (staging/reference)** — marcador 0 bytes que
  Hadoop/Spark crea por cada partición nueva al escribir con el writer nativo
  de Spark (`df.coalesce(1).write.parquet(path)`, usado en
  `visa/calculate/calculate.py` y `visa/interchange/interchange.py`). Se
  regenera en cada corrida nueva — borrar los existentes no sirve de nada
  sin cambiar el mecanismo de escritura. Investigado 2026-07-16: MC no lo
  sufre porque `mastercard/calculate/calculate.py` (`save_parquet`, linea
  ~1374) evita el writer nativo de Spark — hace `toPandas()` + escribe con
  PyArrow directo al path exacto (sin pasar por el committer de Hadoop que
  crea el marcador) — pero esa tecnica no es segura para Visa por volumen
  (`toPandas()` colapsa todo a memoria del driver; los archivos de Visa
  cubren la corrida completa, no un mensaje individual como MC). **La
  alternativa que sí escalaria:** el patron `write_single_parquet()` de
  `mastercard/interchange/interchange.py` (el mismo que ya se le agrego
  `try/finally`) — escribe con el writer nativo de Spark (distribuido, sin
  colapsar a pandas) a un prefijo temporal, y solo copia el part-file
  resultante al nombre final via `copy_object` (boto3, sin pasar los datos
  por el driver). Aplicar ese mismo patron a `visa/calculate.py`/
  `visa/interchange.py` eliminaria los `_$folder$` sin el riesgo de OOM de
  la tecnica de MC-calculate. No evaluado aun si vale la pena el esfuerzo
  vs. el beneficio (es cosmetico, 0 bytes, no rompe nada hoy).

---

## Cleanup legacy (convención antigua)

- [ ] **Revisar los 3 "legacy sin reemplazo" — NO son igual de descartables**
  (verificado 2026-07-13, leyendo el contenido real de los 3, no solo el
  nombre):
  - `CHANGELOG.md`: sin valor, congelado en v1.0.0 (2026-04-08), nunca se
    actualizó. Todo lo que dice ya lo cubren `decisions.md`/`gotchas.md`/
    este mismo archivo de forma viva. Borrar sin reemplazo, esto sí está
    confirmado.
  - `.env.example`: **SÍ sirve** — ya tiene casi toda la convención
    actual (`itl-0004-itx-dev-intchg-02-*`) y el `README.md` raíz lo
    referencia en el paso de Deploy (`cp .env.example .env`). Solo 2
    fixes, no borrar: el ARN de Step Function apunta al viejo
    `itx-main-orchestrator` en vez de `sfn-vi`/`sfn-mc` (y falta variable
    para la 2da SFN), y falta `DYNAMODB_TABLE_MASTERCARD_FIELDS`.
  - `infrastructure/deploy.sh`: dudoso, no descartar sin decidir. Cubre
    solo ~30% de la infraestructura actual (nada de Mastercard, ARDEF/
    IAR, exchange-rates, 7 de los 9 Glue jobs, 1 sola Step Function) con
    nombres viejos — no reproduce el pipeline real si se corre hoy. Pero
    Terraform TAMPOCO está completo (ver el pendiente de
    `stepfunctions.tf` abajo) — hoy ningún camino de IaC despliega el
    pipeline completo a un ambiente nuevo, que es justo lo que va a
    necesitar el pendiente de "Ambiente empresarial" más abajo. Decidir:
    ¿actualizar deploy.sh a la paridad actual, o abandonarlo a favor de
    completar Terraform? No decidir por default a "borrar sin más".
- [ ] **`infrastructure/terraform/stepfunctions.tf`** — define 1 SFN
  (`itx_main_orchestrator`) pero AWS tiene 2 (`sfn-vi`, `sfn-mc`).
  Actualizar a 2 recursos antes de reescribir.
- [ ] **Renombrar crawlers/databases Glue con prefijo `itx-` consistente**
  — los 16 objetos planeados en `glue/GLUE_CATALOG_CREATION.md` existen
  pero con nombres reales que omiten `intchg`. Ver sección "Estado de
  verificación" de ese archivo.
- [ ] **Carpeta `sqs/` completamente vacía** (verificado 2026-07-15) — el
  pipeline actual no usa SQS (reemplazado por eventos S3 + Step
  Functions, ver CLAUDE.md). Candidato a eliminar si se confirma que es
  remanente del scaffolding inicial — no decidido todavía.

---

## Ambiente empresarial

- [ ] **Testing end-to-end en ambiente empresarial** — pendiente cuando
  el ambiente esté disponible.
