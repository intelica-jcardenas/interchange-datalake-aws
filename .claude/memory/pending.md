# Pendientes del proyecto

Checklist vivo — solo tareas activas de **desarrollo de código/pipeline**
(no sincronización ni documentación de scripts — eso se trackea en la
memoria de usuario: ver `push_sync_scripts_design.md` e
`itx_document_script_skill.md`). Lo resuelto se documenta en
`decisions.md`/`gotchas.md` (o en la memoria de usuario correspondiente)
y se borra de aquí — no se acumulan items completados con `[x]`.
Última actualización: 2026-07-31.

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
- [ ] **`vi-data-quality`**: `NumberOfWorkers=10`/`MaxConcurrentRuns=1`
  vs sus pares de reportería (`2`/`1`) — dejado sin tocar a pedido
  explícito del usuario, no confirmar sin que lo pida.

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
