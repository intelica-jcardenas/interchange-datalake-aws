# Gotchas y problemas conocidos

## vi_data_quality.py (BASEII) usaba interchange_fee_currency para convertir itx_amt — mismo error de moneda que MC, ya corregido en get_transaction.py — FIX APLICADO LOCALMENTE, SIN DESPLEGAR NI VALIDAR

**Archivo:** `glue/scripts/reports/vi_data_quality/vi_data_quality.py` (funciones `join_with_exchange_rates` y `aggregate_results`, sección BASEII — el bloque VSS de `aggregate_vss_results` no está afectado).
**Detectado:** 2026-07-14, aplicando a Visa el mismo proceso de revisión que a MC (ver gotcha de `get_transaction.py`/`mc_data_quality.py` arriba).

**Causa raíz:** `itx_amt` se calculaba con `COALESCE(X2.exchange_value, X1.exchange_value, 1) * interchange_fee_amount_itx`, donde `X2` convierte `interchange_fee_currency` (moneda de la regla) → `report_currency`. Pero `interchange_fee_amount_itx` (= `T3.calculated_value`) ya está en `source_currency` desde siempre (decisión validada "dirección del exchange_value", `decisions.md`) — nunca estuvo en `interchange_fee_currency`. El código venía heredado de "Standard 1.0" (legacy), que sí tenía el fee en la moneda de la regla — nunca se actualizó al migrar a la convención actual.

**Diferencia con el caso de MC:** acá no hubo ningún rewrite que cambiara el comportamiento — el fee de Visa siempre estuvo en `source_currency`; simplemente `vi_data_quality.py` nunca se alineó con eso al escribirse (a diferencia de `get_transaction.py`, que si usa `xr1_rate` correctamente para Visa desde el principio).

**Riesgo real:** bajo — `glue-vi-data-quality` solo tiene un smoke test (2026-07-08, corrió sin error), nunca se validó a fondo contra legacy para `itx_amt` específicamente. El fix no mueve ningún número ya confirmado.

**Fix aplicado (local, sin desplegar a S3 ni commitear):**
- `join_with_exchange_rates()`: eliminado el join `X2` completo (`exchange_rate_x2`, el join contra `interchange_fee_currency`, sus drops) — solo queda `X1` (`source_currency_code` numérico → `report_currency`).
- `aggregate_results()`: `itx_amt` pasó de `COALESCE(X2.exchange_value, X1.exchange_value, 1)` a solo `COALESCE(X1.exchange_value, 1)`.
- Docstrings de ambas funciones recortados, sin la explicación completa de "Standard 1.0 vs 2.0".
- Verificado con `grep` que `interchange_fee_currency`/`x2_exchange_value` no quedan referenciados como columna Spark en ningún lado (solo texto de documentación).
- `python -m py_compile` sin errores. **Sin ejecutar, sin comparar contra legacy, sin subir a S3/AWS todavía.**

**Estado:** Fix de código aplicado localmente. Pendiente: subir a S3, correr `glue-vi-data-quality` con datos reales y comparar `itx_amt` antes/después para cuantificar la diferencia (esperable: pequeña, dado que la mayoría de transacciones tienen `interchange_fee_currency == source_currency`).

---

## get_transaction.py / mc_data_quality.py asumían calculated_value (MC) en rate_currency — desactualizado tras el rewrite de calculate_mastercard_fee_pyspark — FIX APLICADO LOCALMENTE, SIN DESPLEGAR NI VALIDAR

**Archivos:** `glue/scripts/reports/get_transaction/get_transaction.py` (función `transform_mastercard`, `_fee_currency_mc`), `glue/scripts/reports/mc_data_quality/mc_data_quality.py` (función `get_mastercard_validation_results_transactional`, `x2_currency_join`)
**Detectado:** 2026-07-14, revisando el impacto downstream del rewrite de `calculate_mastercard_fee_pyspark` en `glue-mc-interchange` (commit `0d9ae133`, 2026-07-05/06 — ver `decisions.md`/memoria de usuario `mc_interchange_fee_currency_rewrite.md`).

**Causa raíz:** el rewrite cambió la moneda en la que queda expresado `calculated_fee` (columna que se escribe al ITX como `calculated_value`): antes en `rule_currency` (`rate_currency`), ahora siempre en `trx_ccy`/DE_49 (moneda original de la transacción). Ni `get_transaction.py` ni `mc_data_quality.py` se habían actualizado para reflejar ese cambio — ambos seguían asumiendo que `calculated_value` estaba en `rate_currency`.

**Hallazgo adicional (importante para la decisión):** el SP legacy real (`sql/get_mastercard_transactions.sql`, línea ~122: `AND X2.currency_from = T3.rate_currency`) confirma que legacy **sí** espera el fee en `rate_currency` para archivos OUT — es decir, `get_transaction.py`/`mc_data_quality.py` no estaban "desactualizados" respecto a legacy, estaban REPLICANDO correctamente el comportamiento legacy. El que se desvió de legacy fue el rewrite de `calculate_mastercard_fee_pyspark` (motivado por una investigación local que consideraba `rate_currency` "no útil para reportería/conciliación" — ver memoria de usuario).

**Decisión tomada (2026-07-14, con el usuario):** mantener el rewrite de `calculate_mastercard_fee_pyspark` (ya desplegado, no se toca) y actualizar los 2 reportes para seguir la nueva convención (`trx_ccy`) en vez de revertir el rewrite. **Consecuencia aceptada:** el resultado de `interchange_fees_amount`/validación MC OUT ya NO va a coincidir con el SP legacy en escenarios cross-currency (`rate_currency != trx_ccy`) — antes sí coincidía. Se prioriza consistencia interna con el pipeline nuevo sobre paridad exacta con legacy en este punto.

**Fix aplicado (local, sin desplegar a S3 ni commitear):**
- `get_transaction.py`: `_fee_currency_mc` pasó de `F.when(file_type=="IN", src_currency_alpha).otherwise(rate_currency)` a simplemente `F.col("src_currency_alpha")` (misma moneda que `amount_transaction` para ambos file_type — ya no depende de `rate_currency`).
- `mc_data_quality.py`: `x2_currency_join` (rama OUT) pasó de `"X2.currency_from = T1.rate_currency"` a `"X2.currency_from_code = T1.currency_code_transaction"` (mismo join que `x1_currency_join`, coherente con que ambas monedas ya son la misma).
- **Simplificación adicional (2026-07-14):** una vez que `_fee_currency_mc == src_currency_alpha`, `xr1_rate`/`xr2_rate` quedaban siempre idénticos para MC — y al revisar los otros 2 callers de `_join_exchange_rates()` (Visa BASEII/SMS) se confirmó que **ninguno de los 3 usa `xr2_rate` en su fórmula real** de `interchange_fees_amount` (Visa siempre usó `xr1_rate`; el único uso real de `xr2_rate` en todo el archivo era la línea de MC ya simplificada arriba) — consistente con que el fee de Visa ya está en `source_currency` desde antes (decisión "dirección del exchange_value"), Visa nunca tuvo este problema. Como ningún caller lo necesitaba, se eliminó `xr2_rate`/`fee_currency_col` de raíz: `_join_exchange_rates()` perdió el parámetro `fee_currency_col` (queda `df, xrate_df, src_currency_col, report_currency`) y solo calcula `xr1_rate` — se actualizaron los 3 call sites (`transform_visa_baseii`, `transform_visa_sms`, `transform_mastercard`) para dejar de pasar `fee_currency_col`. La columna `_fee_currency_mc` de MC quedó eliminada por completo (ya no hace falta ni siquiera como alias).
- **`mc_data_quality.py` — limpieza de `rate_currency` muerta (2026-07-14):** al quitarle la referencia a `rate_currency` de `x2_currency_join`, la columna quedaba seleccionada en `t1_norm` sin ningún uso downstream — se eliminó la constante `COL_TRX_RATE_CURRENCY`, su selección en `t1_norm` y el `StructField("rate_currency", ...)` del schema de `read_ipm_1240_operational()` (20 columnas en vez de 21). Verificado con `grep` que no queda ninguna referencia colgante.
- **`mc_data_quality.py` — join X2 redundante eliminado (2026-07-14):** en `get_mastercard_validation_results_transactional`, tras el fix, `x1_currency_join` y `x2_currency_join` quedaron **literalmente idénticos** en ambas ramas (IN: ambos `currency_code_reconciliation`; OUT: ambos `currency_code_transaction`) — mismo patrón que `xr1_rate`/`xr2_rate` en `get_transaction.py`. Se eliminó `x2_currency_join`, el segundo `LEFT JOIN trx_x X2 ...`, y la fórmula de `itx_amt` pasó de `COALESCE(X2.exchange_value, X1.exchange_value, 1)` a solo `COALESCE(X1.exchange_value, 1)`. No afecta `get_mastercard_validation_results_settlement()` (MTI 1644) — esa función usa `amount_net_fee_in_reconciliation_currency_2` (ya denominado en su propia moneda) y nunca tuvo un segundo join, no toca `calculated_value`/reglas IAR en absoluto.
- Ambos archivos verificados con `python -m py_compile` — sin errores de sintaxis. **Sin ejecutar, sin comparar contra legacy, sin subir a S3/AWS todavía.**

**Impacto real hoy (antes del fix, para contexto):**
- `get_transaction.py`: afectaba `interchange_fees_amount` de Mastercard **solo para archivos OUT** (los IN usan `amounts_transaction_fee_7_pds_146_7`, no `calculated_value`, no afectados). El comparativo MC más reciente (2026-06-30) es anterior al rewrite (07-05/06) — no detectó este problema porque en ese momento la asunción todavía era correcta.
- `mc_data_quality.py`: bug latente, sin impacto observado — el job nunca se ha corrido en producción.
- `scheme_fee.py`: **no afectado** — no usa `calculated_value`/`rate_currency` de MC en ningún punto (su `scheme_fees_amount` es la cuota externa, concepto distinto al fee de interchange).

**Si vuelve a aparecer (residual de `interchange_fees_amount` MC OUT vs legacy en transacciones cross-currency):** es el comportamiento esperado tras esta decisión, no un bug nuevo — confirmar que `rate_currency != trx_ccy` en las filas con diferencia antes de investigar otra causa.

**Estado:** Fix de código aplicado localmente. Pendiente: (1) correr un comparativo real contra legacy con este fix para cuantificar el residual cross-currency que se acepta; (2) subir ambos scripts a S3 y desplegar; (3) commitear (lo hace el usuario). Sigue pendiente también lo ya conocido de `calculate_mastercard_fee_pyspark` en sí (ver `pending.md`): validar el fee en trx_ccy contra legacy, limpiar `settlement_currency_u`, revisar residuales ATM.

---

## SMS: interchange_fees_amount +60% de más, concentrado 100% en transaction_type_id=22 (ATM cash withdrawal) — causa en glue-vi-interchange, no en get_transaction.py — PENDIENTE

**Detectado:** 2026-07-09, validando `transform_visa_sms()` (recién activada, ver gotcha de `xr3_rate` más abajo) contra `analytics.get_visa_sms_transactions()` real, SBSA enero 2026.

**Estado de la validación SMS en `get_transaction.py`:** `count` y `transaction_amount` cuadran **exacto** (1,472,615=1,472,615, $1,059,497,522.78=$1,059,497,522.78, 0.0000% diff) tras el fix de `xr3_rate`. `interchange_fees_amount` da +60.55% de más (621,932.53 nuevo vs 387,370.47 legacy) — pero acotado 100% a `transaction_type_id=22` (ATM cash withdrawal, MCC 6011, processing_code=01): el resto de tipos (247, 249, NULL) y la jurisdiction `off-us` dan fee=0 en ambos sistemas, correctamente.

Desglose del residual (todo en type=22):
| jurisdiction | fees_new | fees_legacy | diff% |
|---|---|---|---|
| interregional | 404,111.37 | 233,773.08 | +72.86% |
| intraregional | 217,821.16 | 153,597.40 | +41.81% |

**No es un bug de `get_transaction.py`** — la fórmula de conversión de moneda para `interchange_fees_amount` (`xr1_rate * interchange_fee_amount`, igual patrón que BASEII, ya validado) es correcta. El problema está en el **cálculo del fee en sí**, hecho por `glue-vi-interchange` (asignación de regla IAR / cálculo de `interchange_fee_amount` para el `type_record` SMS) — mismo motor que BASEII (`evaluate_interchange_fees`/`calculate_fee_amounts`), que sí valida con diff≈0% para BASEII. Causa no identificada aún — candidato a revisar: si algunas transacciones type=22 (ATM) también tienen `source_amount=0` (como el caso ya conocido de `transaction_amount`/cryptogram) y el cálculo del fee en Spark usa `source_amount` crudo en vez del valor ajustado.

**Estado:** Pendiente de investigar y reprocesar si aplica — no bloquea cerrar la validación de `transform_visa_sms()` en `get_transaction.py` (que ya está correcta para count/monto). Ver `.claude/memory/pending.md`.

---

## SMS: transform_visa_sms() reemplazaba el hardcode de USD del legacy por la moneda real del cryptograma — RESUELTO Y VALIDADO

**Archivo:** `glue/scripts/reports/get_transaction/get_transaction.py` (función `transform_visa_sms`)
**Detectado y corregido:** 2026-07-09, primera validación end-to-end de esta función (estaba comentada/nunca activada).

Un intento anterior de esta función reemplazó el hardcode de moneda USD (que el legacy usa siempre, sin excepción, para el fallback de `cryptogram_amount + surcharge_amount_sms` cuando `source_amount=0`: `X3.currency_from_code = '840' -- USD; Special Case` en el SQL) por una búsqueda de la moneda real reportada en `cryptogram_currency_code`. Esto producía montos ~16x menores que el legacy para `business_transaction_type=247` (consulta de saldo en cajero, MCC 6011) en SBSA (moneda local ZAR, tasa ZAR/USD ≈16-18).

**Fix:** revertido a un join fijo `xr3_rate` = USD → report_currency (por fecha, sin depender de ninguna columna de moneda de la transacción), usado solo en el branch `source_amount=0` de `transaction_amount`. `xr1_rate` (antes condicionado a la moneda "efectiva") vuelve a ser siempre `source_currency_code_alphabetic → report_currency`, igual que BASEII.

**Intento fallido en el camino (revertido):** se intentó también hacer que `interchange_fees_amount` usara `xr2_rate` (moneda del fee) con la misma lógica de 3 ramas del SQL legacy (`X2`/`X3` según si `fee_currency = report_currency`) — pero esto causó una **doble conversión**: `glue-vi-interchange` ya normaliza `interchange_fee_amount` a `source_currency` internamente (decisión documentada: "el resultado queda en source_currency"), a diferencia del legacy cuyo cálculo de fee sí queda en `fee_currency` y por eso necesita el join X2/X3 en el reporte. Revertido a la fórmula simple `xr1_rate * interchange_fee_amount`, igual que BASEII (ya validado).

**Validación:** SBSA enero 2026, comparado contra `analytics.get_visa_sms_transactions()` real (no la tabla de prueba legacy, que no incluye SMS) llamada día por día. `count`=1,472,615 exacto, `transaction_amount`=$1,059,497,522.78 exacto (incluye el caso `transaction_type_id=247` que antes fallaba: ahora $5,835,143.23 exacto).

**Estado:** RESUELTO Y VALIDADO para `get_transaction.py`. Ver gotcha arriba para el residual de `interchange_fees_amount` (bug distinto, en `glue-vi-interchange`, aún pendiente).

---

## glue-mc-calculate: calculate_pre2() usa un envelope de 9 dígitos en vez del PAN real para el range-join IAR — explica solo ~11% del gap jurisdiction_code NULL-vs-off-us en MC — PARCIALMENTE INVESTIGADO

**Archivo:** `glue/scripts/mastercard/calculate/calculate.py` (función `calculate_pre2`, líneas ~663-670)
**Detectado:** 2026-07-09, durante revisión de `get_transaction.py` (comparativo agregado SBSA enero 2026, ver `tst_files/reporting/sbsa/comparativo_aggregated_sbsa.md`).

**Síntoma:** agrupando el reporte por `jurisdiction_code` (brand=MC), el conteo total por fecha cuadra exacto (diff=0 en todas las fechas), pero la distribución interna difiere: `jurisdiction_code=NULL` da 249 en el nuevo sistema vs 7,326 en legacy; `jurisdiction_code=off-us` da +6,812 de más en el nuevo. Es decir, ~6,800 transacciones que legacy deja sin clasificar (sin fila en `dh_mastercard_calculated_field_*`, porque `adapters.py` arma esa tabla con **INNER JOIN contra IAR**) el nuevo sistema SÍ las clasifica (con LEFT JOIN, diseño correcto — la discusión no es LEFT vs INNER, ver `decisions.md`/conversación relacionada).

**Causa raíz confirmada (parcial):** `calculate_pre2()` arma el rango de la transacción con un envelope de 9 dígitos — `num_card_low = pan_prefix9 * 10^9`, `num_card_high = pan_prefix9 * 10^9 + 999999999` — y hace un join por **overlap de intervalos** contra IAR, en vez de un punto exacto. Legacy usa el PAN real completo (`left(rpad(pan,18,'0'),18)`) y un chequeo `BETWEEN` puntual. Cuando un prefijo de 9 dígitos tiene rangos IAR angostos que no lo cubren completo (confirmado con ejemplos reales: prefijo `541432330` solo tiene 4 sub-rangos MUS de 10M cada uno dentro de un bucket de 1,000M), el envelope de nuestro sistema matchea igual aunque el PAN real caiga en el hueco — legacy correctamente no matchea (NULL), nosotros sí (asignamos país/jurisdiction real).

**Cuantificado con datos reales (SBSA, 9 archivos MC OUT IPM_1240, 8 fechas distintas de enero, IAR filtrado a la fecha correcta de cada archivo):** 2,012,393 transacciones analizadas, 20 con match falso-positivo (0.0010%) → proyectado al mes completo (~76.9M txns MC) ≈ **764 transacciones, ~11% del gap de 6,812**. Metodología: comparación exacta punto-en-rango (PAN real, `rpad` a 18 dígitos) vs el resultado que produciría el envelope de 9 dígitos, usando el IAR real descargado de `s3-reference/mastercard_iar/historic_data.parquet`.

**Descartado como causa:** diferencia de contenido/cobertura de la tabla IAR — confirmado que el conteo de rangos activos tras filtro+dedup (`active_inactive_code='A'`, `app_date_valid<=file_date`, dedup por `low_key_for_range`) es **idéntico** (186,454) entre nuestro `historic_data.parquet` y la tabla viva `operational.dh_mastercard_iar` en Postgres, a la misma fecha de corte.

**Confirmado que NO lo resuelven los fixes recientes:** se regeneró el reporte completo de SBSA (`glue-get-transaction`, 2026-07-09) sobre datos ya reprocesados con el fix de máscara de PAN y el fix de schema Arrow (`jurisdiction_region`/`settlement_report_amount`) — el comparativo salió **byte-idéntico** al anterior. Esperado: ninguno de esos dos fixes toca la lógica de `calculate_pre2()`.

**Sin explicar (~89% del gap, ~6,000 transacciones):** no se identificó la causa. Se descartó IAR desactualizado; el bug de envelope de 9 dígitos solo cubre una fracción pequeña. Candidato no confirmado: 541 pares de rangos IAR consecutivos que se solapan entre sí incluso después del dedup por `low_key_for_range` (potencial tie-break distinto entre nuestro dedup y el de legacy) — pero ese mecanismo explicaría diferencias de país **entre transacciones que ya matchean en ambos sistemas** (consistente con los residuales pequeños de `intraregional` +202 e `interregional` +63), no el salto NULL→off-us que es el grueso del gap.

**Impacto en dólares:** mínimo — `interchange_fees_amount_diff` de MC es +10,001.28 ZAR sobre 221.9M (0.0045%), ya documentado como residual aceptado en `pending.md`.

**Estado:** Investigación pausada por decisión del usuario — impacto bajo, no justifica seguir cavando ahora. Si se retoma: comparar el dedup IAR fila-por-fila (no solo conteo) entre nuestro sistema y legacy para cerrar el residual de intra/interregional; investigar el 89% restante del gap NULL-vs-off-us (causa aún desconocida).

---

## account_number/pan_de_2 con caracteres de máscara (*/?) sin limpiar — legacy los convertía a '0' — RESUELTO

**Archivos:** `lambdas/visa/clean/src/handler.py`, `lambdas/mastercard/clean/src/handler.py`
**Detectado:** 2026-07-09, comparando `mct_cd` de `scheme_fee.py` contra CSV legacy real.

Visa enmascara los últimos dígitos del PAN con `*`, Mastercard con `?` (byte EBCDIC `0x6F`, decodificación correcta — no es bug de encoding). Legacy (`adapters.py`) reemplazaba esos caracteres por `'0'` al ingerir; nuestro pipeline nunca lo hacía. Fix: en `_clean_chunk()`/`_cast_df()`, reemplazo genérico de cualquier caracter no numérico (`\D`→`'0'`) en `account_number`/`pan_de_2` — no hardcodea el caracter de máscara, para cubrir otras convenciones de red/cliente.

**Estado:** RESUELTO Y VALIDADO. Reprocesado SBSA/202601 completo (clean+store, VI+MC, IN+OUT) el 2026-07-09 — 0 caracteres no numéricos remanentes.

---

## jurisdiction_region/settlement_report_amount con tipo Arrow inconsistente entre bloques — causa raíz real en glue-mc-calculate, no en mc-store — RESUELTO Y VALIDADO

**Archivos:** `glue/scripts/mastercard/calculate/calculate.py` (`save_parquet()`, causa raíz) y `lambdas/mastercard/store/src/handler.py` (`_restore_schema()`, fix complementario).
**Detectado:** 2026-07-09, escaneo de schema-variance de SBSA operational.

Primer intento (solo `mc-store`): `_restore_schema()` solo restauraba tipos degradados por el roundtrip pandas para columnas de CLN (`cln_dtype_map`, capturado con `pq.ParquetFile` antes de pandas) — las columnas nuevas de CAL/ITX se cargaban con `pd.read_parquet()` directo. Fix: nuevo helper `_read_parquet_arrow_s3()` aplicado también a CAL/ITX, combinando `cal_dtype_map`/`itx_dtype_map` con `cln_dtype_map`. Este fix por sí solo **no alcanzó** — reprocesar solo `store` no cambió nada, porque CAL ya venía mal desde el origen.

**Causa raíz real:** `save_parquet()` en `calculate.py` escribe CAL con `df.toPandas()` → `pa.Table.from_pandas(pdf)` **sin schema explícito** — aunque Spark ya castea `jurisdiction_region` a `LongType()` y `settlement_report_amount` a `Decimal(18,4)`, ese cast no sobrevive el paso por pandas: si una columna es 100% NULL en un bloque, pandas la degrada a `float64`; para decimales, PyArrow infiere la precisión mínima que ajuste a los valores de CADA bloque en vez de respetar el `Decimal(18,4)` declarado. Confirmado leyendo el CAL con `pq.read_schema()` (sin pandas): ya salía mal desde `calculate.py`, antes de que `mc-store` lo tocara.

**Fix real:** en `save_parquet()`, derivar el schema Arrow desde `df.schema` (Spark, ya casteado correctamente) con `pyspark.sql.pandas.types.to_arrow_schema()`, forzar timestamps a `us` sin timezone, y pasar ese schema explícito a `pa.Table.from_pandas(pdf, schema=arrow_schema, safe=False)` en vez de dejar que infiera desde los datos.

**Validación:** archivo de prueba conocido (`CEA663ACCB17D5F395B5728D03FAD181`, IPM_1240 OUT 2026-01-02) confirmó `jurisdiction_region: double→int64` y `settlement_report_amount: decimal(7-11,4)→decimal(18,4)`. Reproceso completo `calculate`+`store` SBSA/202601 (104 archivos MC): 104/104 OK ambas etapas. Scan de schema-variance post-reproceso: 0 inconsistencias en IPM_1240 (390 archivos) e IPM_1442 (118 archivos). Crawler `itl_0004_itx_dev_02_glue_crawler_operational_sbsa_mc` re-corrido — catálogo confirma `jurisdiction_region=bigint`, `settlement_report_amount=decimal(18,4)` consistente.

**Si vuelve a aparecer (`double` donde se esperaba `int`, o precisión decimal variable entre archivos) en cualquier columna de CAL/ITX:** sospechar primero de `save_parquet()` (o su equivalente) escribiendo sin schema explícito — mismo patrón recurrente en este proyecto (pandas no tiene int nullable, infiere decimal desde los valores).

Problemas encontrados durante el desarrollo, con su causa raíz y solución recomendada. Verificar si siguen vigentes antes de actuar. Gotchas resueltos y validados con detalle completo (síntoma, debugging, validación paso a paso) fueron movidos a `.claude/memory/gotchas_archive.md` (no cargado automáticamente) — cada entrada resumida abajo tiene su pointer correspondiente.

---

## business_mode: MAYÚSCULA en Visa, minúscula en Mastercard — inconsistencia real entre calculate.py de ambas marcas

**Archivos:** `glue/scripts/visa/calculate/calculate.py` (línea ~454, `calc_business_mode_draft`/`calc_business_mode_sms`) vs `glue/scripts/mastercard/calculate/calculate.py` (línea ~730)
**Detectado:** 2026-07-03, durante el comparativo de `scheme_fee.py` contra legacy (SBSA enero 2026).

**El campo `business_mode` no usa la misma convención de mayúsculas entre marcas:**
- Visa: `F.lit("ACQUIRING")` / `F.lit("ISSUING")` (mayúscula).
- Mastercard: `F.lit("acquiring")` / `F.lit("issuing")` (minúscula).

**Impacto real detectado:** `scheme_fee.py` copió el patrón de Visa (comparación case-sensitive contra `"ACQUIRING"`/`"ISSUING"`) y lo aplicó tal cual a los 3 transforms (Visa BASEII, Visa SMS, Mastercard) sin normalizar el case — para MC, cualquier comparación exacta contra esos literales en mayúscula NUNCA matchea. En este caso concreto rompió (a) el duplicado on-us (nunca disparaba para MC) y (b) el mapeo a códigos legacy en el CSV final (100% de las filas de MC caían en el sentinel de "no matcheó"). Detalle completo → memoria de usuario `scheme_fee_job_design.md`.

**Cómo `get_transaction.py` evita el problema:** no usa la columna `business_mode` de MC directamente — deriva `business_mode_code` desde `file_type` (IN→"I", OUT→"A") en vez de comparar contra el valor de `business_mode`. Por eso el reporting job (ya validado) nunca se topó con esta inconsistencia.

**Si vuelve a aparecer (código nuevo que compare `business_mode` de MC contra literales en mayúscula, o viceversa con Visa en minúscula):** normalizar con `F.upper(F.col("business_mode"))` (o `.lower()`, pero mayúscula es lo que ya usa Visa) ANTES de cualquier comparación/mapeo, en vez de asumir que ambas marcas comparten convención. No se tocó `calculate.py` de ninguna marca — normalizar en el consumidor, no en la fuente, para no romper otros consumidores ya validados que dependan de la convención actual de cada uno.

---

## glue-mc-interchange: monotonically_increasing_id() inestable entre shuffles — fees incorrectos con count/amount OK — RESUELTO Y VALIDADO

**Archivo:** `glue/scripts/mastercard/interchange/interchange.py` (función `assign_rules_simple`)
**Detectado:** 2026-06-25 (comparativo SBSA MC fees vs legacy). Fix desplegado por equipo 2026-06-26.

**Causa raíz:** `assign_rules_simple()` usaba `monotonically_increasing_id()` como llave de join entre transacciones (`work`) y reglas rankeadas (`ranked_rules`). Este ID no es estable entre shuffles Spark — tras el cross-join + `Window.partitionBy(work_id)`, los IDs en `ranked_rules` no correspondían a los de `work` → reglas asignadas a transacciones incorrectas → fees incorrectos.

**Síntoma característico:** conteos y montos de transacciones correctos (diff=0), solo fees difieren. No hay error visible — el job termina SUCCEEDED.

**Fix:** `Window.partitionBy(file_id, file_idn, ref_id)` + join final por esas 3 llaves de negocio estables. Mismo patrón que el merge CLN+CAL+ITX en `lmbd-mc-store`.

**Si vuelve a aparecer:** verificar que `assign_rules_simple()` en S3 NO use `monotonically_increasing_id()`. El síntoma (fees incorrectos, count/amount OK) es idéntico.

**Estado (actualizado 2026-07-01):** RESUELTO Y VALIDADO. Reprocesso SBSA MC enero 2026: 104/104 SUCCEEDED interchange + 104/104 SUCCESS store. Validación de fees ya completada: comparativo `get_transaction.py` del 2026-06-30 da SBSA MC (excl. Jan 16) count=0, fees=+10,001 ZAR (+0.0045%) — residual mínimo, consistente con el fix funcionando. (Nota: esto es independiente del rewrite local sin commitear de la moneda del fee en `calculate_mastercard_fee_pyspark`, ver `mc_interchange_fee_currency_rewrite.md` — ese cambio no está desplegado y no se tocó acá.)

---

## reprocess_mc_store: read timeout botocore ≠ Lambda timeout — falso negativo — CONOCIDO

**Detectado:** 2026-06-28 en reprocesso SBSA MC enero 2026 (2 archivos: Jan 3 IN y Jan 27 OUT).

**Síntoma:** `reprocess_mc_store.py` reporta `EXCEPTION: Read timeout` con `elapsed_s ≈ 910` (el `read_timeout` configurado en botocore). El script marca el archivo como fallido. Sin embargo, el Lambda realmente **terminó con éxito** — la conexión HTTP del cliente expiró antes de recibir la respuesta, pero la ejecución en AWS continuó y completó.

**Verificación:** `aws logs get-log-events` en el stream más reciente de `lmbd-mc-store` muestra `=== Done: N outputs, M records ===` + `REPORT ... Duration: Xs`. Jan 3 IN: 349s, 2,445,146 records. Jan 27 OUT: 356s, 2,535,679 records.

**Causa:** `botocore read_timeout=910s` < tiempo de ejecución del Lambda para archivos grandes (~900s max). La respuesta HTTP llega después del timeout del cliente.

**Solución:** aumentar `read_timeout` en botocore a >900s (ej. `read_timeout=960`) Y/O verificar CloudWatch antes de concluir que falló. El Lambda sí terminó y los datos están escritos en S3.

**Regla:** siempre verificar CloudWatch ante un `Read timeout` en `reprocess_mc_store.py` — no asumir que el Lambda falló.

---

## lmbd-mc-store: OOM en bloques CLN grandes (archivos >500 MB) — RESUELTO Y VALIDADO

**Archivo:** `lambdas/mastercard/store/src/handler.py` (función `_store_output`)
**Detectado:** 2026-06-23 al reprocesar SBSA MC IN 2026-01-03 (1.86 GB, bloque mayor 298 MB CLN)

**Causa raíz:** `_store_output` cargaba el Parquet CLN completo con `pq.read_table().to_pandas()`. Un bloque de 298 MB con ~150 columnas string expandía a >10 GB en pandas, matando el Lambda con `Runtime.OutOfMemory`.

**Fix (2026-06-23):** streaming via `pq.ParquetFile.iter_batches(batch_size=ITX_STORE_BATCH_SIZE, default=100k)`:
- CLN: se descarga completo (bytes) pero se itera en batches de 100k filas; pico por batch ~150 MB
- CAL e ITX: se cargan completos en memoria — son pequeños (~14 MB y ~27 MB)
- Schema de salida: derivado del primer batch (`_restore_schema(batch, cln_dtype_map, output_schema=None)`) y forzado en los siguientes (`output_schema` no-None)
- Escritura incremental a `pq.ParquetWriter(output_buf, schema, "snappy")`, una sola subida S3 al cerrar
- Config: Timeout 300s → **900s** para archivos grandes; `ITX_STORE_BATCH_SIZE` ajustable via env var

**Validación (2026-06-23):** SBSA MC IN 2026-01-03 — 33 bloques, 2,445,146 records, 353s, **2,946 MB** pico (vs ~10 GB antes).

**Si vuelve a aparecer OOM en mc-store:** verificar que el handler en AWS sea la versión streaming (ZIP ≥5.7 KB; función `iter_batches` presente). El bloque problemático suele ser el más grande del MTI 1240 — identificar con `aws s3 ls` por tamaño.

---

## glue-vi-calculate: calc_vss_aggregation_level — lógica recursiva daba nivel 2 a todas las hojas — RESUELTO Y VALIDADO

**Archivo:** `glue/scripts/visa/calculate/calculate.py` (función `calc_vss_aggregation_level`)
**Detectado/corregido:** sync 2026-06-12 (cambio traído de AWS, no documentado hasta ahora)

**Causa raíz:** la implementación anterior navegaba la jerarquía de rollup hacia arriba en 3 iteraciones (`for level in [1, 2, 3]`) para distinguir niveles intermedios. Como el `rollup_to` de cualquier nodo hoja **siempre** pertenece al `rollup_group` (conjunto de todos los `rollup_to != reporting_for`) por definición, la primera iteración (`level=1`) ya marcaba a todas las hojas como nivel 1, y por la forma del loop terminaban en nivel 2 — nunca se asignaba el nivel 0 esperado para hojas.

**Fix aplicado:** reescritura completa a una clasificación directa sin recursión:
- `10` (raíz): `rollup_to == reporting_for`
- `1` (nodo intermedio/padre): `rollup_to != reporting_for` **y** `reporting_for` ∈ `rollup_group` (este nodo es a su vez destino de rollup de otra fila)
- `0` (hoja): `rollup_to != reporting_for` y `reporting_for` ∉ `rollup_group`

Implementado con un único `LEFT JOIN` contra `rollup_group_df` (distinct de `rollup_to` donde `rollup_to != reporting_for`, broadcast) + `F.when(...)` anidado, sin columnas temporales de iteración (`_row_id`, `_current_reporting`, etc. eliminadas).

**Impacto:** afecta `calc_vss_aggregation_level` para VSS_110/120/130/140 — cualquier reporte o validación que dependa de `vss_aggregation_level == 0` (hojas) para filtrar registros de detalle estaba recibiendo `2` en su lugar.

**Si vuelve a aparecer (`vss_aggregation_level` no tiene valores `0`, o tiene `2` donde debería haber `0`):** verificar que el script en S3 (`s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py`) tenga la versión join-based, no la recursiva de 3 iteraciones.

**Estado:** RESUELTO Y VALIDADO 2026-06-18. Reproceso masivo EBGR enero 2026: 105/105 `glue-vi-calculate` SUCCEEDED + 105/105 `lmbd-vi-store` VSS-only SUCCESS + crawler `itl_0004_itx_dev_02_glue_crawler_operational_ebgr_visa` SUCCEEDED. **Pendiente verificado y descartado (2026-07-01):** `vss_aggregation_level` no aparece referenciado en ningún lado de `get_transaction.py` ni de `interchange.py` (`grep` sin resultados) — ninguno de los dos asumía el valor incorrecto, no había nada que corregir ahí.

---

## lmbd-router: extraer_fecha_mc() con _mc_unblock_chunk desalineaba archivos bloqueados con separadores no estándar — RESUELTO

**Archivo:** `lambdas/router/src/handler.py` (función `extraer_fecha_mc`)
**Corregido:** sync 2026-06-12 (cambio traído de AWS, no documentado hasta ahora)

`_mc_unblock_chunk()` descartaba siempre 2 bytes por cada bloque de 1014, sin verificar que fueran un separador válido (`\x40\x40`). En archivos bloqueados con separadores no estándar, esto desalineaba el stream desde el primer bloque "raro" y `_mc_scan_for_695()` nunca encontraba el trailer 695 → `file_date` caía al fallback `datetime.utcnow()` (fecha incorrecta en `file_control`, particiones erróneas en todo el pipeline downstream).

**Fix:** `extraer_fecha_mc()` ahora descarga el archivo completo y usa `_mc_unblock_full()` (replica `unblock_1014()` del interpreter con `valid_seps` pushback). Detalle completo de la decisión y por qué no se pudo mantener el esquema de chunks → `decisions.md` → "Por qué el router extrae la fecha MC descargando el archivo completo".

**Estado:** Resuelto (en AWS desde 2026-06-12). Si vuelve a aparecer `file_date` = fecha de hoy para un archivo MC bloqueado, sospechar de esto primero.

---

## glue-test-1 (glue-vi-mc-reporting): load_exchange_rates() leía tabla incompleta y con columnas incorrectas — RESUELTO Y VALIDADO

**Archivo:** `glue/scripts/reports/get_transaction/get_transaction.py` (función `load_exchange_rates`)

`load_exchange_rates()` leía `exchange-rates/brand={brand}/exchange_date=.../` con columnas inexistentes (`from_currency`/`to_currency`/`fx_rate`) y cobertura incompleta. Fix: lee `exchange_rate/rate_date=YYYY-MM-DD/` (cubre 2025-12-01..2026-04-30, ambas marcas via columna `brand`) y renombra columnas. Validado 2026-06-11: reporte EBGR generado en s3-analytics (561,711 filas, 32 columnas).

**Pendientes que sigue exponiendo:** `scheme_fees_amount` (TODO, flujo no implementado), validación SMS/MC (skeletons `# VERIFY`), escaneo NullType en `SBSA`/`BTRLRO`/`vss_110-140` antes de generar reportes para esos clientes/tipos. `product_program_id` ya resuelto (ver memoria de usuario `visa_bin_products_join.md`).

**Estado:** Resuelto y validado. Detalle completo (síntoma, causa raíz, debugging, validación paso a paso) → `.claude/memory/gotchas_archive.md`.

---

## glue-test-1 (glue-vi-mc-reporting): operational MC (IPM_1240/1442) — TIMESTAMP(NANOS) y tipos inconsistentes entre archivos rompen spark.read.parquet(); fallback PyArrow causa OOM — RESUELTO (entrada corregida 2026-07-01, estaba desactualizada)

**Archivo:** `glue/scripts/reports/get_transaction/get_transaction.py` (función `read_operational`)
**Detectado:** 2026-06-13. **Resuelto:** mismo día, por un fix distinto al propuesto originalmente abajo.

**Síntoma:** `spark.read.parquet("s3://.../EBGR/MC/IPM_1240/")` fallaba en la etapa de scan con `AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))` — la causa física era que `lmbd-mc-store` escribía `date_and_time_local_transaction_de_12` (y otras columnas timestamp) sin schema explícito, dejando que PyArrow infiriera `TIMESTAMP(NANOS)` en vez de microsegundos.

**Fix real aplicado (no el propuesto originalmente — ver nota abajo):** se corrigió en el origen, no en el lector. `lmbd-mc-store` (`_store_output`) ahora restaura el schema Arrow del CLN antes de escribir operational (`_restore_schema()`, forzando `pa.timestamp("us")` en todas las columnas timestamp, tanto heredadas del CLN como nuevas de CAL/ITX) — ver decisión "Por qué lmbd-mc-store restaura el schema Arrow del CLN antes de escribir operational" en `decisions.md`. Con los archivos operational ya escritos con timestamps en microsegundos, `spark.read.parquet()` los lee sin problema — no hizo falta tocar `enableVectorizedReader` ni el resto del workaround propuesto originalmente.

**Limpieza consecuente en get_transaction.py (2026-06-13):** una vez confirmado que el dato físico ya no tenía el problema, se eliminó el código del intento de workaround (`_NANOS_AS_LONG_COLS`, `_widest_arrow_type`, `_align_table_to_schema`, `_read_operational_via_pyarrow`, `spark.sql.legacy.parquet.nanosAsLong` + imports asociados) — `read_operational()` quedó como el `spark.read.parquet()` simple que tiene hoy. Re-validado con `report_suffix=20260102_0105_mc_v3`.

**Validación:** `reprocess_mc_store.py` IN (120/120) + OUT (18/18) SUCCESS sobre EBGR; `scan_mc_operational_schema_variance.py` → 0 inconsistencias en IPM_1240 (138 archivos) e IPM_1442 (5 archivos); comparativos EBGR+SBSA MC de `get_transaction.py` (2026-06-30, ver `project_status.md`) confirman que la lectura MC funciona en producción semanas después del fix.

**Nota sobre datos MUY viejos sin reprocesar:** el fix corrige archivos escritos por `lmbd-mc-store` desde 2026-06-13 en adelante (o reprocesados después de esa fecha). Si algún cliente tiene operational MC de una fecha anterior que nunca fue reprocesado, en teoría podría seguir en NANOS — de aparecer el mismo `AnalysisException`, es la primera sospecha, y el fix es reprocesar ese archivo con `lmbd-mc-store` actual, no tocar el lector.

**Relevante para `glue/scripts/reports/scheme_fee/scheme_fee.py` (2026-07-01):** ese script copia `read_operational()` tal cual desde `get_transaction.py` — al estar el problema resuelto en el origen, no necesita ningún workaround adicional. Se había flaggeado por error como riesgo pendiente en una revisión previa; corregido tras verificar `decisions.md` y el código actual de `lmbd-mc-store`.

---

## lmbd-vi-store: columnas NullType en operational rompen lectura de directorio completo con Spark (SchemaColumnConvertNotSupportedException) — RESUELTO

**Archivo:** `lambdas/visa/store/src/handler.py` (función `store_output`)

Columnas del CAL 100% null para ciertos `file_id` (`message_reason_code`, `type_of_purchase`) se degradaban a NullType (INT32) en el round-trip pandas/pyarrow, rompiendo `spark.read.parquet(directorio)` cuando convivían con archivos donde la columna sí tenía `string`. Fix: generalización de `_cal_int_cols` → `_cal_dtype_map` (restaura NullType→string además de float64→int64). Reprocesados 56/56 archivos `EBGR/VISA/baseii_drafts/file_type=IN` (2026-01-01..2026-01-30) — 0 columnas NullType tras el fix; validado por el re-run de `glue-test-1` (gotcha anterior).

**Caso adicional (2026-06-16) — 3 archivos con `status=PARTIAL_SUCCESS`:** Detectado al comparar el reporte EBGR enero 2026 completo (report_suffix=202601_v2) contra legacy: las fechas 2026-01-20, 2026-01-21 y 2026-01-29 mostraban ~13% de las filas esperadas. Causa: esos 3 archivos habían sido procesados originalmente **antes** de que el fix de `_cal_dtype_map` estuviera desplegado — `output_type=BASEII` falló por NullType durante el procesamiento original y DynamoDB quedó con `status=PARTIAL_SUCCESS` (solo VSS_110/120/130/140 se habían escrito). El `output_type=BASEII` nunca fue escrito en operational. Reprocesados con `lmbd-vi-store` (solo BASEII, CLN/CAL/ITX confirmados presentes en S3): 3/3 SUCCESS. Tras crawler re-run y re-ejecución del reporte: VI count = 4,051,482 / 4,051,482 (diff=0). **Señal de alerta:** si el comparativo con legacy muestra que ciertas fechas tienen ~13% de las filas esperadas (no 0%), verificar `status` en DynamoDB `file_control-02` — puede ser un `PARTIAL_SUCCESS` silencioso, no un error de datos.

**Cómo identificar `PARTIAL_SUCCESS` en DynamoDB:**
```powershell
aws dynamodb get-item `
  --profile itx-dev `
  --table-name itl-0004-itx-dev-dynamo-file_control-02 `
  --key '{"file_id": {"S": "<file_id>"}}' `
  --query "Item.{status:control_status.S, store_result:store_result.S}"
```
Si `store_result.outputs[]` no incluye `output_type=BASEII`, ese output falló.

**Distinción `file_id` vs `content_hash`:** DynamoDB `file_control-02` usa `file_id` como PK (no `content_hash`). Si se tiene solo el `content_hash`, usar `scan` con `filter-expression "content_hash = :h"` para obtener el `file_id` real. En el caso de Jan 20: `file_id=0A8221C3293EF535621FB1E35D709ACC` (PK) pero `content_hash=F308708F2709F2F83AF7C692B33BA292` (distinto).

**Pendiente (actualizado 2026-07-01):** SBSA verificado indirectamente — el comparativo `get_transaction.py` del 2026-06-30 dio SBSA VI count=0/amount=0 (sin el patrón de "~13% de filas esperadas" que delataba `PARTIAL_SUCCESS` en EBGR), consistente con que SBSA no tiene archivos con este problema. `BTRLRO` sigue sin verificar (no forma parte de los comparativos validados hasta ahora) — chequear si/cuando se necesite generar un reporte para ese cliente.

**Si vuelve a aparecer:** usar `tst_files/debug_scripts/scan_nulltype_columns.py` para listar archivos/columnas afectadas, mapear via `file_control` (scan por rango de fechas + `control_status=PARTIAL_SUCCESS`) y reprocesar con `lmbd-vi-store`.

**Estado:** Resuelto y validado para EBGR enero 2026 completo (2026-06-16). Detalle completo (debugging, escaneo, reprocesamiento) → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: fillna(0.0) en fee_min/fee_cap zeroeaba fees positivos — RESUELTO Y VALIDADO

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `process_pandas_partitions`)
**Detectado:** 2026-06-09

**Causa raíz:** `.fillna(0.0)` sobre `interchange_fee_cap`/`interchange_fee_min` convertía `NaN` (reglas sin cap/min) en `0.0`. Spark recibe `0.0` como valor real (no NULL) → `coalesce(0.0, ±inf) = 0.0` → `least(fee_amount, 0.0) = 0`, zeroeando todos los fees positivos de esas reglas (y flooreando los negativos por el lado de `fee_min`). Detectado al comparar `sum(interchange_fee_amount)` por jurisdiction/source_currency vs legacy (off-us EUR: −289 USD).

**Fix aplicado:** Eliminado `.fillna(0.0)` — dejar solo `.astype(float)`. NaN→NULL en Spark→`coalesce(NULL,±inf)`→sin restricción.

**Si vuelve a aparecer:** verificar que no haya `fillna(0.0)` sobre esas dos columnas antes del yield — solo `.astype(float)`.

**Estado (corregido 2026-07-01, esta entrada estaba desactualizada):** RESUELTO Y VALIDADO. Confirmado presente en la versión de `interchange.py` del commit `28df878` (2026-06-30). Evidencia de re-ejecución vía `aws s3api list-objects-v2`: `EBGR/VISA/500_baseii_itx_drafts/` y `EBGR/VISA/baseii_drafts/` (operational) tienen `LastModified=2026-06-29` en prácticamente todos los `file_id` de enero 2026 (58/59 grupos; 1 archivo residual de `date=2026-01-30` quedó en 2026-06-10/12, sin impacto visible). El comparativo `get_transaction.py` del 2026-06-30 (EBGR VI: fees=-1.30 EUR, -0.0003%) confirma el fix funcionando en producción. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: matching incorrecto intelica_id ATM JPY — regla 1055 en vez de 1065 — PENDIENTE

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (motor de reglas `_apply_default` / `_evaluate_rules_pandas`)
**Detectado:** 2026-06-09

**Síntoma:** En la comparación de `sum(interchange_fee_amount)` por jurisdiction/source_currency, la diferencia residual de −29.64 para interregional JPY (source_currency=392) se debe a que el nuevo sistema asigna `intelica_id=1055` ("ATM AF") mientras el legacy asigna `intelica_id=1065` ("ATM AF JPN").

**Detalle de las reglas vigentes al 2026-01-03:**

| intelica_id | fee_descriptor | fee_variable | fee_fixed | fee_currency |
|---|---|---|---|---|
| 1055 | ATM AF | 0.0015 | — | None (source_ccy) |
| 1065 | ATM AF JPN | 0.0015 | 0.50 | USD |

Simulación para source_amount=20,220 JPY:
- Legacy (1065): `0.0015 × 20,220 × exchange(JPY→USD) + 0.50 = 0.19 + 0.50 = 0.69 USD`
- Nuevo (1055): `0.0015 × 20,220 = 30.33 JPY`

Nota: los fee_amounts están en **monedas distintas** (USD vs JPY) — no son comparables como número directo.

**Causa probable:** La regla 1065 "ATM AF JPN" tiene alguna condición que la restringe a transacciones de Japón (issuer_country, acquirer_country, o merchant_country). Esa condición existe en `visa_rules` pero el motor de reglas del nuevo sistema no la está evaluando correctamente o no está presente en el `calculate.parquet` para esa transacción.

**Para investigar:** Comparar los campos de condición entre la regla 1065 y la 1055 en `visa_rules.parquet` (ambas vigentes al 2026-01-03) para identificar qué campo diferencia "ATM AF JPN" de "ATM AF". Verificar que ese campo tenga el valor correcto en `calculate.parquet` para la transacción en cuestión.

**Estado:** Pendiente de investigación. La diferencia de −29.64 en la comparación global es 1 transacción (count=1).

---

## glue-vi-interchange: dirección del exchange_value — RESUELTO Y VALIDADO (entrada corregida 2026-07-01, estaba desactualizada)

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `calculate_fee_amounts`)
**Detectado:** 2026-06-09. **Resuelto:** 2026-06-30 (commit `28df878`).

**Contexto (histórico):** Existían dos fórmulas posibles para `interchange_fee_amount`, con resultados distintos en transacciones cross-currency:

| Sistema | Fórmula | Moneda del resultado |
|---|---|---|
| Legacy PostgreSQL | `fee_variable × (source_amount × exchange_value) + fee_fixed` | fee_currency |
| Prototipo local | `fee_variable × source_amount + fee_fixed × exchange_value` | source_currency (si exchange_value = source_ccy/fee_ccy) |

El usuario prefería que el fee se exprese en **source_currency** ("la regla se adapta a la moneda de la transacción").

**Fix aplicado:** el join contra `exchange_rate` en `calculate_fee_amounts` ahora es explícito `(from=fee_ccy, to=source_ccy)` — `exchange_value = rate(fee_ccy → source_ccy)`, usado para convertir `fee_fixed`/`fee_min`/`fee_cap` de `fee_ccy` a `source_ccy` antes de aplicar `fee = fee_variable × source_amount + fee_fixed_convertido`. Queda comentado explícitamente en el código (`# Join direction: (from=fee_ccy, to=source_ccy)...`), resolviendo la ambigüedad — el resultado queda en `source_currency`, como prefería el usuario.

**Estado:** RESUELTO Y VALIDADO. Confirmado en el código actual (mismo commit `28df878` del 2026-06-30 usado en el comparativo EBGR/SBSA de esa fecha: EBGR fees=-1.30 EUR/-0.0003%, SBSA fees=+68,285 ZAR/+0.038% con residual ya explicado por ATM NO AF). Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-calculate: calc_timeliness_draft fórmula de domingos tenía off-by-one — no cuadraba con legacy — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py` (función `calc_timeliness_draft`)
**Detectado:** 2026-06-09

**Causa raíz:** La fórmula original (`full_weeks + extra_sunday` con `extra_sunday = when(remaining >= days_to_next_sunday, 1)`) contaba un domingo de más cuando `remaining == days_to_next_sunday` (ese domingo cae justo fuera de la ventana `[purchase+1, central-1]`).

**Fix aplicado:** Reescritura a fórmula directa con offset: `offset = (8 - start_dow) % 7`; `sundays = max(0, floor((total_days - 1 + 6 - offset) / 7))`.

**Si vuelve a aparecer:** la discrepancia (timeliness 1 de menos que legacy) aparece solo cuando `(total_days - 1) % 7 == (8 - start_dow) % 7` — cualquier lógica `remaining >= days_to_next_sunday` tiene este off-by-one.

**Estado (actualizado 2026-07-01):** Resuelto en código, incluido en el `calculate.py` desplegado — CAL de EBGR enero 2026 tiene `LastModified=2026-06-18` (posterior al fix), y el interchange/operational reprocesado el 2026-06-29 usa ese CAL. No hay una validación numérica DIRECTA de la columna `timeliness` contra legacy, pero el comparativo de fees EBGR del 2026-06-30 (-0.0003%) es evidencia indirecta fuerte: `timeliness` alimenta la clasificación de reglas de interchange sensibles al tiempo, así que un error ahí produciría discrepancias de fee mucho mayores a las observadas. Si se quiere confirmar directamente, comparar la columna `timeliness` de `calculate.parquet` contra el valor legacy fila a fila. Detalle completo (derivación, ejemplo numérico) → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: _apply_default() convertía NaN a cadena "nan" en columnas no-SPACE — filas excluidas de reglas válidas — RESUELTO

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `_apply_default`)
**Detectado:** 2026-06-09

**Causa raíz:** `batch[col].astype(str).str.strip()` convierte `NaN → "nan"` (len=3), por lo que `.mask(len==0, "BLANK")` no lo sustituye — la columna queda con la cadena `"nan"`, que no matchea contra `valid_values` (e.g. `['Y','N','BLANK']`) y la fila cae al fallback.

**Fix aplicado:** `batch[col].fillna("").astype(str).str.strip()` antes del `.mask(...)` — garantiza `NaN → "" → "BLANK"`.

**Si vuelve a aparecer:** verificar que toda normalización de condiciones use `fillna("").astype(str).str.strip()`, nunca `astype(str)` directo sobre columnas con nulls.

**Estado (corregido 2026-07-01, esta entrada estaba desactualizada):** RESUELTO Y VALIDADO. Confirmado presente en `interchange.py` (commit `28df878`, 2026-06-30) — `_apply_default` usa `batch[condition_name].fillna("").astype(str).str.strip()`. Misma evidencia de re-ejecución que la entrada de fillna(0.0) arriba (S3 `LastModified=2026-06-29` en interchange/operational EBGR + comparativo 2026-06-30 con residuos mínimos). Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: content_hash se perdía en el Parquet ITX por mapInPandas — RESUELTO Y VALIDADO

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `evaluate_interchange_fees`)
**Detectado:** 2026-06-08

**Causa raíz:** `evaluate_interchange_fees()` usa `mapInPandas()`, que reemplaza el schema completo — `content_hash` llegaba como columna de entrada (propagado desde clean/calculate) pero no estaba declarado en `OUTPUT_COLS`/`output_schema`, así que se descartaba silenciosamente. Job terminaba SUCCESS, conteo correcto, sin la columna.

**Fix aplicado:** agregado `"content_hash"` como primer elemento de `OUTPUT_COLS` y `StructField("content_hash", StringType(), True)` como primer campo de `output_schema`.

**Si vuelve a aparecer (columna ausente pese a estar en la lista de columnas finales):** sospechar de un `mapInPandas`/`applyInPandas` intermedio que reemplaza el schema — la columna debe declararse tanto en la salida del iterador como en el `StructType`.

**Estado (corregido 2026-07-01, esta entrada estaba desactualizada):** RESUELTO Y VALIDADO. Confirmado presente en `interchange.py` (commit `28df878`, 2026-06-30) — `content_hash` es primer elemento de `OUTPUT_COLS` y del `output_schema`. Misma evidencia de re-ejecución que la entrada de fillna(0.0) arriba. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-interchange: _apply_default() destruía el token "Space" (espacio literal) — transacciones GR caían en regla fallback — RESUELTO Y VALIDADO

**Archivo:** `glue/scripts/visa/interchange/interchange.py` (función `_apply_default`)
**Detectado:** 2026-06-08

**Causa raíz:** un `value = value.strip()` extra (no presente en el prototipo local validado) convertía el espacio literal `' '` en `''` al parsear criterios tipo `"Space,9"` → `valid_values=['','9']`. Transacciones GR con `acceptance_terminal_indicator=' '` (que está en `COLUMN_GROUP_SPACE`, sin normalizar) no matcheaban `intelica_id=39` ("GR SECURE CR") y caían en el fallback `63` ("GR NON-SEC CR"). Validado contra producción: 524 transacciones GR cumplían TODAS las demás condiciones de la regla 39.

**Fix aplicado:** eliminado el `.strip()` extra dentro del loop de `value_list`.

**Si vuelve a aparecer:** verificar que ningún `.strip()`/normalización adicional se aplique a `value_list` después de `replace("SPACE", " ")` — el espacio literal debe sobrevivir hasta el `isin()`.

**Estado (corregido 2026-07-01, esta entrada estaba desactualizada):** RESUELTO Y VALIDADO. Confirmado presente en `interchange.py` (commit `28df878`, 2026-06-30) — el loop de `value_list` en `_apply_default` ya no tiene el `.strip()` extra. Misma evidencia de re-ejecución que la entrada de fillna(0.0) arriba; no se re-contaron específicamente las 524 transacciones GR, pero el fee residual EBGR global (-0.0003%) es consistente con el fix funcionando. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-calculate: load_visa_ardef() vaciaba el ARDEF por to_date() sin formato — campos ARDEF quedaban 100% null — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py` (función `load_visa_ardef`)

`F.to_date(F.col("effective_date"))` sin formato (campo real es `yyyyMMdd`) devolvía `NULL` para el 100% de las filas, vaciando el ARDEF y dejando los 10 campos derivados (`ardef_country`, `product_id`, etc.) 100% null. Fix: `F.to_date(F.col("effective_date"), "yyyyMMdd")` + eliminación de un pre-filtro de strings con formatos incompatibles.

**Estado:** Resuelto y validado (re-run 2026-06-06, match ~100% vs ARDEF local; reprocesado masivamente para EBGR enero 2026 en 2026-06-11). Detalle completo (metodología de comparación valor-a-valor) → `.claude/memory/gotchas_archive.md`.

---

## mc-interpreter: mensaje IPM con DE_55 corrupto desincronizaba el stream y abortaba el archivo completo — RESUELTO

**Archivo:** `lambdas/mastercard/interpreter/src/handler.py` (función `read_len_prefixed_messages_variable`)
**Detectado:** 2026-06-10

**Causa raíz:** Un `DE_55` con longitud declarada (120) distinta de la real (118 bytes) — anomalía del archivo fuente — desincronizaba la lectura de DEs subsecuentes. El handler anterior no tenía manejo de error: `KeyError`/`ValueError` no controlados abortaban el generador completo, perdiendo TODOS los bloques ya procesados (porque `finalize_writers`/`upload_tmp_outputs` solo corren si el generador termina sin excepción).

**Fix aplicado:** se portó el mecanismo de resync del legacy (`_resync_stream` + `_valid_mti_byte_patterns`, parametrizado por encoding cp500/latin-1): ante cualquier fallo de parseo de DE (`parameters.get(i)` sin KeyError, `parse_ok=False; break` en todos los casos de error), se busca el siguiente mensaje válido escaneando `record_length` + MTI plausibles; si no se encuentra, se hace `break` preservando los bloques ya procesados (a diferencia del `on_error=True` del legacy que descartaba todo el archivo).

**Validación (2026-06-10):** lectura completa del archivo de prueba (422,734 mensajes), 100% `parse_ok=True`, 2 mensajes corruptos consecutivos descartados via resync.

**Si vuelve a aparecer (lectura se detiene antes del final / `KeyError`/`ValueError` no controlado):** revisar log `WARNING ... Mensaje corrupto descartado ... RESYNC exitoso/fallido`; si el resync falla repetidamente cerca del mismo offset, sospechar corrupción real del archivo fuente.

**Estado (corregido 2026-07-01, esta entrada estaba desactualizada):** RESUELTO Y VALIDADO. `aws lambda get-function-configuration` confirma `LastModified=2026-06-13` para `lmbd-mc-interpreter` (posterior al fix del 2026-06-10) — el handler con resync está desplegado. `project_status.md` (memoria de usuario) marca `lmbd-mc-interpreter` como "✓ Funcional (fix resync stream corrupto)", consistente con los comparativos MC end-to-end de EBGR/SBSA del 2026-06-30 (que dependen de que el interpreter haya funcionado correctamente como primer paso del pipeline). Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## mc-transform: timeout con múltiples MTIs (riesgo alto)

**Archivo:** `lambdas/mastercard/transform/src/handler.py`
**Detectado:** 2026-05-22

**Problema:** El handler procesa los 4 MTIs (1240, 1442, 1644, 1740) secuencialmente en una sola invocación. Si todos están presentes en el archivo, puede superar fácilmente el timeout de 400s.

**Solución recomendada:** Que Step Functions invoque el Lambda una vez por MTI, pasando el MTI como parámetro — igual que el patrón ya usado en el flujo Visa.

**Estado:** Pendiente de resolver antes de validación end-to-end.

---

## mc-transform: sin chunking en MTIs 1442, 1740 y 1644 (riesgo medio)

**Archivo:** `lambdas/mastercard/transform/src/handler.py`
**Detectado:** 2026-05-22

**Problema:** Solo `transform_ipm_1240` implementa chunking dinámico. Los MTIs 1442, 1740 y 1644 cargan el Parquet completo en memoria, lo que puede causar OOM en archivos grandes.

**Solución recomendada:** Replicar el patrón de chunking de `transform_ipm_1240` en los otros tres MTIs.

**Estado:** Pendiente.

---

## mc-transform: EphemeralStorage /tmp insuficiente (riesgo medio)

**Archivo:** `lambdas/mastercard/transform/src/handler.py`  
**Config:** `lambdas/mastercard/transform/config.json`
**Detectado:** 2026-05-22

**Problema:** `transform_ipm_1240` escribe un Parquet completo en `/tmp` antes de subirlo a S3. El EphemeralStorage por defecto es 512 MB, insuficiente para archivos Mastercard grandes.

**Solución recomendada:** Aumentar EphemeralStorage a 2048 MB+ en la config del Lambda, o cambiar la escritura para hacer stream directo a S3 (sin pasar por `/tmp`).

**Estado:** Pendiente.

---

## mc-transform: variable de entorno DDB_MASTERCARD_FIELDS_TABLE no declarada en config.json (bug latente)

**Archivo:** `lambdas/mastercard/transform/config.json`
**Detectado:** 2026-05-22

**Problema:** El código usa `DDB_MASTERCARD_FIELDS_TABLE` para consultar la tabla de campos Mastercard en DynamoDB, pero esta variable no está declarada en `config.json` ni en `env-vars.json`. Cae al valor hardcodeado `"itl-0004-itx-dev-dynamo-mastercard_fields-02"`, lo que romperá en ambientes distintos a dev.

**Solución recomendada:** Agregar `DDB_MASTERCARD_FIELDS_TABLE` a `config.json` y `env-vars.json` igual que las otras variables de entorno del Lambda.

**Confirmado aún pendiente (sync 2026-06-11):** El sync de Lambdas de esta sesión trajo cambios de código a `lmbd-mc-transform` (nuevo `content_hash` propagado + filtro `list_parquet_files` por `file_id`, ver `decisions.md`), pero el diff de `config.json` solo modificó `CodeSize`, `LastModified`, `CodeSha256` y `RevisionId` — el bloque `Environment.Variables` quedó sin cambios y `DDB_MASTERCARD_FIELDS_TABLE` sigue ausente. El bug latente sigue vigente.

**Estado:** Pendiente — bug latente que se manifestará al desplegar en ambiente empresarial.

---

## itx-extract comparte el rol IAM del router (deuda técnica)

**Detectado:** 2026-04-08 (CHANGELOG v1.0.0)

**Problema:** `lmbd-vi-extract` no tiene un rol IAM propio — comparte `itx-lambda-router-role`. Esto viola el principio de mínimo privilegio.

**Solución recomendada:** Crear `itx-lambda-extract-role` con solo los permisos que extract necesita (S3 read/write staging, DynamoDB read visa-fields).

**Estado:** Pendiente (documentado en CHANGELOG como tarea para el nuevo ambiente).

---

## glue-vi-calculate: Py4JError causado por toPandas() en load_visa_ardef — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py`

`load_visa_ardef` usaba `.toPandas()` + dedup/eliminación de solapamientos en pandas, presionando la heap del driver (OOM con archivos grandes) → `Py4JError` en la siguiente llamada a `logger.info()`. Fix (2026-06-02): migración completa a Spark (`Window.partitionBy` + `row_number()`/`F.lag()`), el ARDEF nunca sale de los executors.

**Estado:** Resuelto. Si reaparece `Py4JError` en este job, buscar `Java heap space`/`ExecutorLostFailure` en CloudWatch justo antes. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-mc-interchange: filtra por file_id para no reprocesar ejecuciones anteriores

**Archivo:** `glue/scripts/mastercard/interchange/interchange.py`

Sin filtro por `file_id`, el job reprocesaba TODOS los Parquets de la partición `file_type=X/date=YYYY-MM-DD`, incluyendo los de ejecuciones anteriores del mismo día. Fix: filtrar archivos listados por `stem_from_uri(path).upper().startswith(file_id.upper())` (aplica a TXN/CLN y CAL). Mismo patrón aplicado en `lmbd-mc-transform` (ver `decisions.md`).

**Estado:** Resuelto, por diseño desde la implementación inicial. Verificar el mismo patrón en `glue-vi-interchange` si aparece el mismo síntoma. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-calculate: timeliness debe ser LongType (bigint), NO IntegerType — HIVE_PARTITION_SCHEMA_MISMATCH — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py`

`.cast(IntegerType())` en `calc_timeliness_draft`/`calc_timeliness_sms` escribía `int` (INT32) en Parquets nuevos mientras particiones viejas tenían `bigint` (INT64) → Athena `HIVE_PARTITION_SCHEMA_MISMATCH`. Fix: `.cast(LongType())` en ambas funciones — todos los archivos quedan `bigint`.

**Estado:** Resuelto. Si reaparece, verificar que el script en S3 use `LongType()` y, si hay particiones mixtas, forzar `bigint` en el catálogo antes de re-crawl. Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-vi-calculate: tipos explícitos en funciones de cálculo numérico — RESUELTO

**Archivo:** `glue/scripts/visa/calculate/calculate.py`

Sin `.cast()` explícito, Spark infiere tipos que el crawler de Glue detecta incorrectamente (`double` en vez de `int`). Casts aplicados (2026-06-05): `calc_business_transaction_type_draft`/`calc_reversal_indicator_draft`/`calc_reversal_indicator_sms` → `IntegerType()`; `calc_surcharge_amount` → `DoubleType()`; `calc_timeliness_draft`/`calc_timeliness_sms` → `LongType()` (ver gotcha anterior).

**Regla:** toda nueva función de cálculo numérico debe terminar con `.cast(TipoExplícito)`.

**Estado:** Resuelto. Detalle completo (tabla) → `.claude/memory/gotchas_archive.md`.

---

## lmbd-vi-store: columnas enteras del CAL se escriben como double en operational — RESUELTO

**Archivo:** `lambdas/visa/store/src/handler.py`

`pq.read_table(...).to_pandas()` convierte INT64+nulls → `float64` (numpy no tiene int nullable); al reconstruir con `pa.Table.from_pandas()`, PyArrow infiere `double`, y el crawler detectaba `timeliness` (y otras columnas enteras del CAL) como `double` en vez de `bigint`/`int`. Fix (2026-06-05): leer CAL con `_read_parquet_arrow()`, capturar `_cal_int_cols` (tipos enteros del schema Arrow) y restaurar `float64 null → int64 null` con `.cast()` tras cada `pa.Table.from_pandas(merged)`.

**Estado:** Resuelto — generalizado después a `_cal_dtype_map` (ver gotcha de NullType en lmbd-vi-store arriba, también cubre NullType→string). Detalle completo → `.claude/memory/gotchas_archive.md`.

---

## glue-mc-interchange: solo procesa MTIs 1240 y 1442 (1644 y 1740 excluidos)

**Archivo:** `glue/scripts/mastercard/interchange/interchange.py`
**Detectado:** 2026-06-02

**Comportamiento:** El job llama a `run_interchange_mti()` únicamente para MTIs 1240 y 1442. Los MTIs 1644 (liquidación) y 1740 (fee collection) no tienen capa ITX generada por este job.

**Impacto en mc-store:** `MTIS_WITH_ITX = frozenset({"1240", "1442"})` — el store no intentará buscar `600_IPM_1644_ITX` ni `600_IPM_1740_ITX`, lo que es correcto.

**Estado:** Por diseño. No es un bug. Ver decisión en `decisions.md` sobre por qué no se contrasta contra 1644.

---

## lmbd-vi-clean: _parse_dates() lógica incorrecta para campos de fecha YDDD y MMDD — RESUELTO

**Archivo:** `lambdas/visa/clean/src/handler.py` (función `_parse_dates`)

La estrategia "compute-then-correct" (restar años si el resultado supera `file_date`) era incorrecta para los 3 formatos: `!YDDD` (`central_processing_date`/`account_reference_number_date`) restaba 10 años de más; `!MMDD` (`purchase_date`) comparaba fecha completa en vez de solo el mes; `conversion_date` necesitaba un nuevo formato `!YDDD_MAX` (cap a 1 año atrás si supera `file_date`). Reescritura completa de `_parse_dates()` con las 3 estrategias correctas + DynamoDB actualizado (`conversion_date.date_format = !YDDD_MAX`).

**Esquema real de `visa_fields-02`:** HASH=`type_record`, RANGE=`column_name` (CLAUDE.md decía `field_id` — corregido).

**Estado:** Resuelto y validado al 100% contra PostgreSQL legacy (0 nulls, 8/8 combinaciones campo/fecha coinciden), desplegado 2026-06-08. Detalle completo (tabla de validación, señales de regresión) → `.claude/memory/gotchas_archive.md`.

---

## Athena HIVE_BAD_DATA: columnas ARDEF (ardef_country, etc.) BINARY en Parquet vs integer en partición — RESUELTO

**Tabla:** `itl_0004_itx_dev_02_glue_database_operational_ebgr_visa.baseii_drafts`, partición `file_type=IN/date=2026-01-15`

Antes del fix de ARDEF (gotcha de `load_visa_ardef()` arriba), los 10 campos derivados eran 100% null y el crawler los tipó como `int` en la metadata de esa partición. Tras el fix, el Parquet físico tiene esas columnas como `BINARY` (string), pero la partición seguía con `int` en el catálogo → `HIVE_BAD_DATA`. Fix: re-correr el crawler (`UPDATE_IN_DATABASE`).

**Si vuelve a aparecer (`HIVE_BAD_DATA ... BINARY ... incompatible with int/double/etc.`):** comparar `aws glue get-partition` vs `get-table` para la columna/partición afectada y re-correr el crawler correspondiente. Causa típica: columna que fue 100% null por un bug ya corregido.

**Estado:** Resuelto y verificado en `operational_ebgr_visa` y `staging_ebgr_visa` (2026-06-10). Detalle completo → `.claude/memory/gotchas_archive.md`.
