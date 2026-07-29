# Pendientes del proyecto

Checklist vivo — solo tareas activas de **desarrollo de código/pipeline**
(no sincronización ni documentación de scripts — eso se trackea en la
memoria de usuario: ver `push_sync_scripts_design.md` e
`itx_document_script_skill.md`). Lo resuelto se documenta en
`decisions.md`/`gotchas.md` (o en la memoria de usuario correspondiente)
y se borra de aquí — no se acumulan items completados con `[x]`.
Última actualización: 2026-07-28.

---

## Refresh visa_rules (excel V37) — cerrado por ahora, 2026-07-28

Ver `.claude/memory/decisions.md` ("Refresh de visa_rules desde excel V37...")
para el detalle completo. Código desplegado, parquet nuevo subido a S3,
validado sin crash contra datos reales (EBGR + SBSA). Pendiente real:

- [ ] **Commitear** `calculate.py`/`interchange.py` (los 3 fixes:
  `token_requestor_id`, `settlement_flag` en drop_cols SMS, `cashback`) —
  lo hace el usuario.
- [ ] Decidir si se reprocesa EBGR/SBSA completos (enero 2026) con
  `visa_rules` nuevo, o se deja que el próximo archivo que llegue ya use
  la tabla nueva sin tocar lo ya procesado.
- [ ] Decidir cuándo borrar el backup
  `s3://itl-0004-itx-dev-intchg-02-s3-reference/visa_rules_backup_pre_v37_20260728/data.parquet`
  (dejado por seguridad, no es parte del flujo normal).
- [ ] **MC — bug real encontrado en el camino, sin fix:** `token_flag` en
  `glue-mc-interchange` (`interchange.py` línea ~595) se deriva de
  `electronic_commerce_indicator_2_pds_52_2` (campo equivocado). Legacy
  (`adapters.py` línea 4128) lo deriva de `token_requestor_id` (`left(...,3)='501'`
  → 1/0). El campo correcto (`token_requestor_id_pds_59` en
  `mastercard_fields`) solo está definido para MTI 1644/1740, no para
  1240/1442 (los que sí evalúa `glue-mc-interchange`) — falta el PDS/tag
  correcto para esos 2 MTIs antes de poder arreglarlo. Sin cuantificar
  impacto (1,119 reglas usan `token_flag='0'`/`'1'` como condición).
- [ ] MC: excel `MASTERCARD Reglas Intercambio V23.xlsx` también revisado
  (`tst_files/interchange_rules/`) — sin columnas de criterio nuevas
  (mismo schema de 32 columnas), pero con contenido actualizado (673
  altas, 628 bajas, cambios reales en `rate_variable`/`token_flag`/
  `fee_tier`/etc.). `mc_rules_new.parquet` generado localmente
  (`output/`), sin subir a S3 — bloqueado hasta resolver el bug de
  `token_flag` de arriba (no tiene sentido refrescar reglas que dependen
  de un campo mal calculado).

---

## Pipeline Visa — residuales conocidos (bajos, aceptados)

- [x] **ATM NO AF + ATM DCC NO AF (SBSA):** CERRADO 2026-07-20/21 — legacy
  excluye estas transacciones de toda regla ATM por un bug de su propia
  formula de `surcharge_amount` (depende de `business_format_code`, que
  viene vacio) combinado con el motor de reglas que descarta `'BLANK'`
  de las columnas `column_group_greater_less`. No es un bug nuestro.
  Detalle → `.claude/memory/gotchas.md` y memoria de usuario
  `vi_vs_legacy_differences_sbsa.md` (Hallazgo 1).
- [x] **Swap intraregional↔interregional (SBSA, ~+452/-458):** CERRADO
  Y VALIDADO 2026-07-22 — `country/data.parquet` tenia `visa_region_code`
  incorrecto para Belarus (5, debia ser 7=CEMEA) y Martinica (0, debia
  ser 5=Europe). Corregido + reprocesado end-to-end (calculate→
  interchange→store→get_transaction) para SBSA VI enero 2026. Swap
  resuelto casi al 100% (bloque IN: 0 exacto; bloque OUT: residuo minimo
  de -4/-2 sin investigar). Detalle → `.claude/memory/gotchas.md` y
  memoria de usuario `vi_vs_legacy_differences_sbsa.md` (Hallazgo 4).
  **Nota:** el fix es global (afecta a cualquier cliente/marca Visa que
  se reprocese despues) pero solo se reproceso SBSA — EBGR no se toco.
- [x] **Swap CR/DB CP↔CNP (SBSA, off-us, ~1,572-1,980 transacciones) —
  CERRADO Y CUANTIFICADO 2026-07-25.** Causa: bug `adapters.py` linea
  5129 (`NOT:1,2,...,9` mal parseado, moto_eci_indicator='1' nunca
  excluido en legacy). Cuantificado corriendo `check_moto_bug_full_month.py`
  (NEW, reescrito a boto3 nativo — `s3fs` se colgaba sin error a mitad de
  lote) + `legacy_full_month_cp_cnp.py` (LEGACY, se le agrego
  `SET ENABLE_PARTITIONWISE_JOIN TO ON;` — sin eso lleno el disco temporal
  de PRD con `DiskFull` la primera vez, corregido y validado con 1 fecha
  antes de correr el mes completo) contra todo enero 2026: el bug explica
  **el 100% del swap de conteo** (IN: -41 exacto = Hallazgo 2a; OUT: -3,511
  exacto = CR -1,572 + DB -1,939) y **casi el 100% del $ residual de
  off-us** (+$6,096 vs +$6,089 esperado, diff de solo $7). Detalle
  completo → memoria de usuario `vi_vs_legacy_differences_sbsa.md`
  (Hallazgo 2/2a).
- [x] **Swap `product_id` en A/interregional y A/intraregional (SBSA,
  post Hallazgo 4) — CERRADO 2026-07-24/25 vía batch automatizado, 100% explicado.**
  Tras el fix de Belarus/Martinica (Hallazgo 4), quedaban 10 pares de reglas
  que se pisan (`PREMIUM CARD`/`PREMIUM ALT`/etc. ganando vs `NON PREMIUM
  CARD`/`BUS ALT`/etc. perdiendo, mas 5 pares CEMEA en intraregional). Se
  corrio `tst_files/debug_scripts/batch_product_id_swap_review.py` (par por
  par, ver metodologia abajo) sobre los 10 pares — **133 cuentas
  discrepantes analizadas**:

  | Mecanismo | Cuentas | % |
  |---|---|---|
  | **freshness** (ARDEF actualizado el mismo dia, legacy congelado — mismo mecanismo que Hallazgo 3) | 72 | 54% |
  | **deleted_range** (rango ARDEF solapado borrado gana el desempate de legacy — bug real, ver mas abajo) | 31 | 23% |
  | **unknown → 30/30 explicadas por Hallazgo 5b abajo (bug NUESTRO)** | 30 | 23% |

  (Numeros corregidos 2026-07-24 tras arreglar un bug del propio script:
  `classify_account()` re-simulaba `new_product_id` con una version
  simplificada de `load_visa_ardef()` en vez de usar el valor YA
  MATERIALIZADO en los datos operational -- corregido para usar el valor
  real, primeros numeros habian sido 67/31/35.)

  Por par (`tst_files/debug_scripts/batch_swap_review_summary.md`/`_detail.csv`):
  `PREMIUM ALT`/`BUS ALT` y `PREMIUM CARD`/`COMCL-BUS` (los 2 pares mas
  grandes) 100% freshness. `SPR PREMIUM CARD`/`NON PREMIUM CARD` dominado
  por deleted_range (mas 8 cuentas Hallazgo 5b, grupo `415159xxx`).
  **`CEMEA GOLD`/`CEMEA INF` es 100% Hallazgo 5b (21 cuentas)**. Los 3
  pares CEMEA mas chicos dieron 0 en todas las fechas (magnitud <10
  transacciones/mes, sin señal detectable a nivel de cuenta).

  Los 2 mecanismos confirmados:
  1. **Freshness de ARDEF** (mismo mecanismo que Hallazgo 3): una version
     nueva de `product_id` con `effective_date` = mismo dia del archivo
     — legacy quedo congelado con la version vieja. Confirmado en
     `intelica_id=623 (PREMIUM ALT)` vs `785 (BUS ALT)`, 100% concentrado
     en 2026-01-27 (+147/-147 exacto) — cuenta ejemplo `4463032140000000`
     (`product_id`: NEW="N" vs LEGACY="G3"; corriendo la SQL de desempate
     de legacy CONTRA EL ARDEF ACTUAL da "N", igual que nosotros —
     confirma que no es un bug de logica, es timing de carga).
  2. **Rangos ARDEF solapados donde legacy no descarta el borrado**
     (bug real de legacy): dos rangos con distinto `low_key_for_range`
     que se solapan, uno con `delete_indicator='D'`, el otro activo. El
     desempate de legacy (`ROW_NUMBER() OVER (PARTITION BY t.app_id,
     t.app_hash_file ORDER BY app_date_valid DESC, high_key_for_range
     DESC)`) no filtra por `delete_indicator` y el rango borrado (con
     `high_key_for_range` mas grande) gana. Confirmado en `account_number
     4627222400000000` (`product_id`: NEW="C" vs LEGACY="F", intelica_id
     `PREMIUM DGD SPR`/`NON...`).

  **Metodologia final validada (v3, par por par -- NO comparar todas las
  familias de un bloque a la vez, genera miles de falsos positivos, ver
  gotcha abajo):**
  1. Diff neto por familia (mes completo) → armar pares (ganadora,
     perdedora) por magnitud combinada descendente (emparejamiento greedy).
  2. Para cada par, filtrar SOLO esas 2 familias, encontrar la(s) fecha(s)
     donde se concentra la diferencia.
  3. Por cada fecha, usar SOLO los sub-`intelica_id` con diferencia ESE dia
     (no el rango completo de ~100 ids de la familia) y comparar el
     **conjunto exacto de ids por cuenta** (inner join, no outer) — 3
     iteraciones previas fallaron por: (a) comparar sub-id exacto con TODOS
     los ids del bloque a la vez (8-10K falsos positivos: ruido de cuentas
     con actividad legitima en otras reglas el mismo dia), (b) agrupar por
     "familia dominante" con el rango completo de la familia (diluye la
     señal de la transaccion puntual que sí swapea entre docenas de otras
     transacciones legitimas de la cuenta ese dia — daba 0 con 31 cuentas
     conocidas reales), (c) requerir dominante no-nulo en ambos lados via
     outer join (elimina señal real ademas del ruido). **Gotcha aparte
     tambien encontrado:** `step2_athena` no casteaba `intelica_id` a int
     (quedaba string vs int nativo de legacy) — comparar tuplas de ids
     nunca daba match aunque fueran el mismo valor (325/325 "discrepantes"
     falsos). ARDEF (nuevo y legacy, sin dedup) se carga UNA vez en memoria
     al inicio, lookups de cuenta 100% locales con pandas (nada de
     round-trips S3/Postgres por cuenta).

  Scripts reutilizables: `tst_files/debug_scripts/batch_product_id_swap_review.py`
  (v3 final, parametrizable via el dict `BLOCKS`), `find_intelica_ids_a_interregional.py`,
  `ardef_lookup_*.py`/`ardef_history_*.py` (drill-down manual puntual).
  Detalle completo → memoria de usuario `vi_vs_legacy_differences_sbsa.md`.
- [ ] **BUG NUESTRO confirmado: `load_visa_ardef()`/`join_with_ardef()`
  (calculate.py) resolvian mal los rangos ARDEF solapados — REDISEÑO
  APLICADO 2026-07-27, VALIDADO END-TO-END 2026-07-28 (Spark real,
  reproceso SBSA 156/156, comparativo final contra legacy), PENDIENTE
  SOLO COMMIT.** Encontrado investigando el "unknown" del batch
  de arriba: confirmado en 30/30 cuentas "unknown" (21 `CEMEA GOLD`/
  `CEMEA INF`, 9 `SPR PREMIUM CARD`/`NON PREMIUM CARD`+`SPR PREMIUM
  ALT`/`NONPREMIUM ALT`, grupo `415159xxx`) — el 100% del "unknown" es
  este mecanismo, no legacy.
  **3 iteraciones del fix, la 3ra es la version final:** v1 (self-join
  a nivel de ARDEF, desempate `low_key_for_range ASC`) → v2 (mismo
  self-join, desempate ajustado a `table_key DESC` para calzar con
  legacy) → **v3 (2026-07-27, REDISEÑO):** al revisar el adapter real de
  legacy (`tst_files/python_scripts/adapters.py`, ~linea 2285-2306 y
  2382, a pedido del usuario) se encontraron 2 problemas de fondo que
  v1/v2 no resolvian: (a) el dedup por `low_key_for_range` (antes paso
  6) no tenia desempate real (ordenaba por la misma columna de la
  particion) — con datos reales de CEMEA sobrevivian 2 filas con el
  mismo `low_key_for_range` y cual ganaba era no-deterministico en
  Spark, pudiendo dejar el self-join del paso 7 sin ninguna fila
  correcta que priorizar; (b) el self-join a nivel de ARDEF resuelve mal
  el anidamiento a 3+ niveles con cobertura parcial distinta (puede
  descartar un rango que sigue siendo el ganador correcto para el
  subconjunto de cuentas que el rango mas nuevo no cubre).
  **Causa de fondo:** `load_visa_ardef()` intentaba pre-resolver el
  ARDEF sin solapamientos de forma GLOBAL antes de tocar transacciones.
  Legacy nunca hace esto — dedupea solo por `low_key_for_range`
  (dejando rangos solapados), hace INNER JOIN de transacciones contra
  TODOS los candidatos, y resuelve el ganador POR TRANSACCION despues
  del join (`ROW_NUMBER() OVER (PARTITION BY app_id, app_hash_file
  ORDER BY app_date_valid DESC, high_key_for_range DESC)`).
  **Fix v3:** `load_visa_ardef()` pasos 5+6+7 colapsados a un solo dedup
  por `low_key_for_range` (`ORDER BY effective_date DESC, table_key
  DESC`, replica `ardef_pre_r` de legacy) — el resultado PUEDE seguir
  teniendo rangos solapados, ya no se eliminan aqui; `effective_date` se
  agrega a los campos seleccionados (antes se descartaba).
  `join_with_ardef()`: el dedup post-join que YA EXISTIA cambia su
  desempate de `ardef_country DESC NULLS LAST` (arbitrario) a
  `effective_date DESC NULLS LAST, table_key DESC NULLS LAST` (replica
  el `ROW_NUMBER()` real de legacy).
  **Validado local (pandas, sin Spark/AWS):** ambos casos conocidos
  (CEMEA `402824050/060`, `415159xxx`) resuelven correcto y
  deterministico -- `P`→`I` y `L`→`F` respectivamente, usando el
  historial ARDEF real (`ardef_history_402824.py`,
  `ardef_history_415159.py`).
  **Validado en Spark real (2026-07-28):** archivo real de SBSA
  (`file_id=AB21B95BFF579E50318D74C9449A89EC`, OUT, 2026-01-06, cubre
  los 3 casos conocidos a la vez) procesado con `glue-vi-calculate`
  (`jr_f84e8113...`, SUCCEEDED, 136s) -- CAL resultante confirma
  `402824050`→`I`, `402824060`→`I`, `415159016`→`F` (antes `P`/`P`/`L`),
  exacto lo que predijo la validacion local en pandas.
  **Reproceso masivo completado (2026-07-27/28):** SBSA VI IN+OUT enero
  2026 (156 archivos) reprocesado completo -- `reprocess_vi_calculate.py`
  → `reprocess_vi_interchange.py` → `reprocess_vi_store.py` (los 3 ya
  preconfigurados para este rango) -- **156/156 SUCCEEDED en las 3
  etapas, 0 fallos.** Verificado antes de lanzar interchange que los 156
  CAL tenian `LastModified` fresco (cruzando cada entrada del log contra
  su ruta real en S3, no solo muestra) -- 0 problemas.
  **Comparativo final contra legacy (2026-07-28):** `glue-get-transaction`
  regenerado sobre el operational reprocesado (`report_suffix=
  sbsa_202601_hallazgo5bfix`) vs `analytics.report_transactions_
  sbsa_202601_tst` -- `A/interregional` count_diff -4→+0 (exacto);
  `A/intraregional` fee_diff -1,842.80→+28.41 (practicamente exacto,
  `CEMEA GOLD` ya no aparece en la tabla de diferencias); fee total
  off-us +6,089.08, coincide EXACTO con lo predicho independientemente
  por Hallazgo 2. Grupo `415159xxx`: hipotesis original (product_id=F
  moveria estas 9 cuentas a `SPR PREMIUM CARD`) resulto incorrecta --
  siguen en `NON PREMIUM CARD`/`NONPREMIUM ALT`, pero verificado que
  legacy TAMBIEN las clasifica ahi (mismo filtro `issuer_bin_8=
  41515901` corrido contra la tabla legacy) -- coincide exacto, no es
  un problema real, solo un ajuste al modelo mental de la traduccion
  product_id→interchange_rule para este grupo.
  **Cuantificacion final agregada (2026-07-28):** residuo total SBSA VI
  enero 2026 = $70,836.66 sobre $179,743,131.61 procesados (0.039%).
  98.4% ($69,667) ya explicado por Hallazgo 1 ($63,578.29, bug legacy
  `surcharge_amount`) + Hallazgo 2/2a ($6,089.08, bug legacy
  `moto_eci_indicator`) -- ambos confirmados con precision de linea de
  codigo en `adapters.py`. El 1.6% restante (~$1,170) son efectos de
  timing (freshness, no bugs) + lineas "sin explicar" de magnitud ≤20
  txn cada una. Comparativos versionados y renombrados a pedido del
  usuario: `tst_files/reporting/sbsa/comparativo_vi_jurisdiccion_
  negocio_reglas_v{1,v2_byfix,v3_hallazgo5bfix}.md` (v3 = vigente, con
  columna Motivo heredada de v2 y actualizada).
  **Pendiente explicito:** (1) commitear -- unico bloqueante real; (2)
  **EBGR nunca se toco durante toda esta investigacion** (decision
  explicita del usuario) -- el fix ya esta desplegado globalmente
  (mismo script S3 sirve a cualquier cliente), pero EBGR nunca se
  comparo contra legacy con este mismo rigor; sin decidir si vale la
  pena repetir el ejercicio ahi; (3) impacto en performance a escala
  sin medir (el reproceso de 156 archivos corrio bien, sin timeouts,
  pero sin comparacion explicita de tiempo antes/despues). Detalle
  completo → memoria de usuario `vi_vs_legacy_differences_sbsa.md`
  (Hallazgo 5b, seccion "Cierre final").
- [x] **9 cuentas "unknown" restantes del batch — CERRADO 2026-07-25,
  mismo mecanismo (Hallazgo 5b), Hallazgo 5 cierra al 100% (133/133).**
  Grupo `415159011/012/013/014/016` (`SPR PREMIUM ALT`/`NONPREMIUM ALT`:
  1, `SPR PREMIUM CARD`/`NON PREMIUM CARD`: 8). La investigacion anterior
  (2026-07-24) habia quedado sin cerrar porque una replicacion manual de
  los pasos 5-7 predecia `product_id='F'` sin coincidir con el valor real
  observado (`'L'`). Re-verificado 2026-07-25 con historial ARDEF crudo
  real (`tst_files/debug_scripts/ardef_history_415159.py`, boto3 directo
  contra `visa_ardef/data.parquet` + `operational.dh_visa_ardef`): mismo
  patron de anidamiento que CEMEA — rango ancho de 2022-01-27
  (`415159000-415159999`, `product_id=L`) conteniendo un rango angosto de
  2023-08-02 (`415159010-415159029`, `product_id=F`). Replicando el paso 7
  VIEJO (buggy) con esas 2 filas exactas se reproduce EXACTO el valor real
  `'L'` — la replicacion manual anterior tenia un error. Confirma que es
  el mismo bug de `load_visa_ardef()` paso 7, no un tercer mecanismo.
  Detalle completo → memoria de usuario `vi_vs_legacy_differences_sbsa.md`
  (Hallazgo 5b).
- [ ] **ATM JPY rule 1055 vs 1065 (EBGR, 1 transacción):**
  `glue-vi-interchange` asigna la regla equivocada, fee_fixed=0.50 USD
  faltante. Investigar campo diferenciador en `visa_rules`.

---

## Pipeline Mastercard — residual conocido (bajo, aceptado por ahora)

- [x] **jurisdiction_code NULL-vs-off-us (SBSA, MC) — CAUSA RAÍZ
  ENCONTRADA 2026-07-20, no es un bug del pipeline nuevo.** Consultado
  directo PRD (`dh_mastercard_calculated_field_sbsa_*`, 62 tablas de
  enero): de las 7,326 filas `jurisdiction=NULL` en legacy, solo 416
  son por falta real de match IAR — **6,910 SÍ tienen `iar_country`
  resuelto** (`ZAF`/CEMEA), pero legacy no puede clasificarlas porque
  `card_acceptor_country_code` (DE 43, país del comercio) está **vacío
  en la tabla legacy** (verificado con JOIN a `dh_mastercard_data_element_*`)
  — el join de legacy contra `m_country` falla con código vacío →
  jurisdiction=NULL. Verificado que **nuestro CLN nunca tiene ese campo
  vacío** (1.64M filas reales revisadas, 0 blancos) — el parser IPM
  nuevo extrae correctamente donde el de legacy lo deja en blanco. El
  número cuadra exacto: 6,812 (NULL en legacy, dirección IN) = la misma
  cifra que "off-us +6,812 de más" del gap original. Envelope de 9
  dígitos (~974) + este hueco de legacy (~6,812+) explican casi el
  100% del gap. **Verificado por MTI+dirección** (1240 IN/OUT completo
  31.7M filas, 1442 IN/OUT completo): nuestro CLN replica el mismo
  hueco en solo 2 de ~7,187 casos (0.03%, ambos en 1442 OUT) — residual
  insignificante. **No hay nada que corregir en el pipeline nuevo** —
  si acaso, legacy subestima off-us por su propio hueco de extracción.
  Detalle completo → `.claude/memory/gotchas.md`.

---

## Pipeline Mastercard — cambios desplegados sin validar

**Diff revisado y COMMITEADO 2026-07-20** (`git diff` de los 8 archivos revisado antes del commit): los 4 scripts de código (`interchange.py`, `get_transaction.py`, `mc_data_quality.py`, `vi_data_quality.py`) coincidían exacto con lo documentado abajo — sin código de debug, sin cambios inesperados. Los 4 compilaron limpio (`py_compile`). Los 2 `args.json` modificados eran solo cambios de `TempDir` (reorganización de paths en AWS vía sync, trivial). Commiteado por el usuario: `bf0dc16` ("Update 20260717 - Fix Currency Transaction", 8 files changed, 365 insertions(+), 243 deletions(-)).

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
  coincidía). Subido a S3 y commiteado (`bf0dc16`, 2026-07-20).
  Comparativo real contra legacy: HECHO 2026-07-17 para SBSA (ver bullet
  de "Plan de despliegue" abajo) — falta EBGR (bajo valor, ver ese mismo
  bullet). Detalle completo → `.claude/memory/gotchas.md`.
- [ ] **vi_data_quality.py — mismo fix de moneda aplicado a Visa
  (2026-07-14), sin desplegar ni validar.** `itx_amt` (BASEII) usaba
  `interchange_fee_currency` para convertir el fee, cuando
  `interchange_fee_amount_itx` ya está en `source_currency` desde
  siempre (no hubo rewrite en Visa, el script nunca se alineó con esa
  convención). Corregido a usar solo `X1`/`xr1_rate` (misma tasa que
  `trx_amt`), eliminado el join `X2` redundante. Riesgo bajo — el job
  solo tiene un smoke test, nunca se validó `itx_amt` a fondo contra
  legacy. Subido a S3 y commiteado (`bf0dc16`, 2026-07-20). Falta:
  correr con datos reales y comparar antes/después — no se ha vuelto a
  ejecutar desde el fix. Detalle completo → `.claude/memory/gotchas.md`.
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
- [x] **Revisión de detalle MC completada 2026-07-20:** `interchange_rule=6-YG`
  (mayor mover individual, -10,462.43 ZAR) validado como correcto —
  fórmula `rate_variable×monto + rate_fixed_USD×FX(USD→trx_ccy)`
  confirmada contra `mc_rules.parquet` y transacciones reales
  (converge a `rate_variable` exacto en montos grandes, FX implícita
  realista en montos chicos). El delta vs legacy es la divergencia
  cross-currency ya aceptada, no un bug. De paso: `BEFORE_feefix.parquet`
  no servía para join fila-a-fila con `AFTER` (`file_id` de MC cambió de
  convención en `8f2183c`, 2026-07-10 — ver gotcha nueva), y se
  confirmó que `get_transaction.py`/`scheme_fee.py` YA usan la misma
  convención de llave (`content_hash`) — no hay bug de cruce GT↔SF.
  **Precisión: NO es 100% — solo `6-YG` (la pieza más grande) se
  verificó individualmente.** Las demás reglas que compensan el
  residual global (`6-61`, `6-YX`, `9-YG`, `9-61`, etc.) se asumen por
  analogía, sin chequear una por una. El gap `jurisdiction_code`
  NULL-vs-off-us sigue sin resolver (~89%, problema distinto de join,
  no de moneda). EBGR sigue sin validar con datos reales. **Sigue
  Visa** (el residual conocido ATM NO AF/DCC de arriba, +68,285
  ZAR/+0.038%).

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

## Housekeeping tst_files/ (limpieza parcial hecha 2026-07-20)

- [x] Limpieza parcial de `tst_files/` (14 GB → 6.3 GB): borrado el
  duplicado exacto `reporting/sbsa/report_transactions_SBSA_sbsa_202601.parquet`,
  `reprocessing/__pycache__/`, logs vacíos de SMS, la investigación
  cerrada `mc_interchange_fee_review/` + `mc_fee_currency/` (rewrite de
  moneda MC, ya commiteado en `bf0dc16`), y el parquet crudo de
  `scheme_fee_reports/union_test/` (4.19 GB, ya resumido en su `.md`).
  **A pedido del usuario, se conservaron intactos**
  `reporting/sbsa/report_transactions_SBSA_sbsa_202601_{BEFORE,AFTER}_feefix.parquet`
  — quiere darles una revisión final manual antes de decidir si
  también se borran. No asumir que ya se limpiaron sin confirmar.

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
