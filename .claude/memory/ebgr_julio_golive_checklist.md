# EBGR — checklist de preparación para carga real de Julio 2026 (primer cliente en producción)

Documento puntual para la reunión de coordinación con el equipo — **separado
de `pending.md`** (que trackea deuda técnica ya conocida del pipeline, no
gaps operacionales de puesta en producción). Verificado en AWS el
2026-08-02. No confundir con el reproceso de `token_flag`/`visa_rules` V37
ya cerrado para enero 2026 (ver `decisions.md`) — este documento es sobre
lo que falta para que la carga de **archivos nuevos y reales** de julio
funcione correctamente por el flujo automático.

---

## 1. Datos de referencia con vigencia por fecha — DESACTUALIZADOS, sin trigger automático

Verificado en S3 (`aws s3api head-object`/`ls`):

| Fuente | Última actualización | Gap al 2026-08-02 | Usado por |
|---|---|---|---|
| `exchange-rates-glue/` (Visa + Mastercard) | 2026-07-14 | ~19 días | `glue-vi-interchange`, `glue-mc-interchange`, `glue-mc-calculate`, `glue-get-transaction` |
| `visa_ardef/data.parquet` | 2026-06-05 | ~2 meses | `glue-vi-calculate` (`load_visa_ardef`/`join_with_ardef`) |
| `mastercard_iar/{data,historic_data}.parquet` | 2026-05-18 / 2026-06-01 | ~2-2.5 meses | `glue-mc-calculate` (`prepare_iar`/`calculate_pre2`) |

**Confirmado:** ninguna de las 3 fuentes tiene un trigger automático
(`aws events list-rules` no muestra ningún schedule para
`lmbd-vi-exchange-rates`/`lmbd-mc-exchange-rates`/`glue-exchange-rates`,
ni para los Lambdas ARDEF/IAR) — solo se refrescan cuando alguien las
invoca manualmente.

**Riesgo real:** varios de estos joins tienen fallback silencioso
(`coalesce(...,1)` en tipo de cambio, `LEFT JOIN` en ARDEF/IAR) — un
archivo de julio con fecha fuera de la cobertura actual **no va a fallar
con error**, va a procesar con datos viejos/incompletos sin avisar.

**Para decidir con el equipo:**
- ¿Quién es dueño de mantener estas 3 fuentes al día en producción?
- ¿Se refrescan a mano antes de cada carga, o se arma un schedule
  (EventBridge) para que corran solas?
- Refrescar las 3 antes de procesar cualquier archivo real de julio.

---

## 2. Flujo automático real (router → Step Functions → archive) — RESUELTO 2026-08-13, validado con archivo real de EBGR y demostrado ante el equipo

**Actualización 2026-08-13:** se subió un ZIP real de EBGR
(`VISA260702.zip`, 8 archivos: 5 VISA/INCOMING + 3 sin match) a
`s3-landing` sin ninguna invocación manual. Las 5 ejecuciones reales de
`sfn-vi` que disparó el router terminaron `SUCCEEDED`
(Transform→Extract→Clean→Calculate→Interchange→Store→Archive),
`s3-operational` con Parquets reales, `s3-landing` limpio, los 3 sin
match correctamente registrados `UNKNOWN`/archivados (ver
`decisions.md`/`gotchas.md` del proyecto — el mismo día se corrigió que
`lmbd-unzip` tenía su propia lógica de matching independiente de la del
router). La demo formal ante el equipo al día siguiente usó este mismo
mecanismo y salió bien — limitada a mostrar el procesamiento formal de
archivos una vez llegan a landing; `s3-analytics`/`scheme-fee`/
`data-quality` quedan para una sesión futura. **Este ítem queda
cerrado** — detalle histórico del gap original abajo, sin acción
adicional.

---

### Detalle histórico (síntoma original, verificado 2026-08-02, ya resuelto)

Confirmado en `s3-archive` (`EBGR/originals/...`, ordenado por
`LastModified` real): el archivo más reciente que pasó por el flujo
completo (landing → router → Step Function → store → archive) fue
procesado el **2026-06-16** — más de 6 semanas sin actividad real.

**Importante:** todo el reproceso de `token_flag`/`visa_rules` V37 hecho
en esta sesión (SBSA + EBGR, enero 2026) fue por **invocación manual
directa** de los Glue jobs/Lambdas (scripts en `tst_files/reprocessing/`),
sin pasar por el router ni las Step Functions. Eso confirma que el
código de cada etapa funciona, pero **no confirma que el flujo
automático completo funcione hoy** para un archivo nuevo llegando a
landing.

**Para decidir con el equipo:**
- Hacer un smoke test end-to-end real: un archivo entra a
  `s3-landing/EBGR/...`, se deja que el router lo clasifique y dispare
  la Step Function correspondiente sin intervención manual, y se
  confirma que llega a `s3-operational` y se archiva correctamente.
- Confirmar que `lmbd-router`, las Step Functions (`sfn-vi`/`sfn-mc`) y
  el resto de Lambdas del flujo automático están con el código más
  reciente desplegado (no solo los 2 scripts que tocamos hoy).

---

## 3. Servidor SFTP (AWS Transfer Family) — mecanismo desconocido, no documentado

Encontrado en `aws events list-rules`:

```
Name: itl-0004-itx-dev-lmbd-file-load-02-sftp-rule
EventPattern: server-id=s-ec4a3c1bb3a04bb88
              detail-type: ["SFTP Server File Upload Failed", "SFTP Server File Upload Completed"]
              source: aws.transfer
```

**Lo que NO sabemos (sin permisos para verlo / no documentado en el
repo):**
- Qué Lambda es el target real de esta regla.
- Si este SFTP es el mecanismo por el que van a llegar los archivos
  reales de EBGR en julio, o si es de otro flujo/cliente/ambiente.
- Si escribe directo a `s3-landing` (lo que dispararía el router
  normalmente) o si hay algún paso intermedio que no conocemos.
- Quién administra las credenciales/usuarios SFTP del lado del cliente.

**Para preguntar al equipo mañana, en este orden:**
1. ¿Este SFTP es el canal real de ingesta para EBGR?
2. Si sí — ¿a dónde escribe? ¿Ya está probado con algún archivo real?
3. ¿Quién lo administra / tiene acceso a ver el Lambda target y logs?

**Hallazgo adicional (2026-08-18, revisión del inventario de migración a PRD de Infra):** aparecieron más piezas del mismo rompecabezas, con un prefijo de nomenclatura distinto al del proyecto (`itl-0004-itx-dev-` sin `-intchg-02-`):
- Roles IAM: `itl-0004-itx-dev-tf-sftp-02-landing-role`, `...-landing-pre-role`, `...-logging-role`.
- Buckets S3: `itl-0004-itx-dev-s3-landing-02`, `itl-0004-itx-dev-s3-landing-pre-02` (y otros: `raw`, `structured`, `enriched`, `configuration`, `devops`, `log`, `operational`, `scheme-fee`, todos con sufijo `-02` y sin `intchg`).
- Coincide con el `lmbd-file-load-02` ya visto arriba (mismo prefijo `itl-0004-itx-dev-lmbd-file-load-02-role`, confirmado que existe como rol IAM real).

**No confirmado si esto escribe a nuestro `s3-landing` real** (`itl-0004-itx-dev-intchg-02-s3-landing`) — el nombre "landing-pre" sugiere una etapa previa/de staging del lado SFTP antes de que el archivo llegue a landing real, pero no se verificó el flujo completo (permisos insuficientes para ver el Lambda target, mismo bloqueo que el hallazgo original de arriba). Mismas 3 preguntas para el equipo, ahora con más pistas concretas para acotar la búsqueda.

---

## 4. Hallazgo aparte: EBGR ya tiene archivos de febrero 2026 sin el reproceso de hoy

`file_control-02` (DynamoDB) tiene ~13 archivos MC de EBGR con
`file_processing_date` en 2026-02-01/02-02 (`status=DONE`) — procesados
con el código/reglas **viejos**, nunca entraron al reproceso de
`token_flag`+`mc_rules` V23 (ese quedó limitado a enero 2026, a pedido
explícito). No hay archivos VI ni de otros meses (marzo–junio) más allá
de enero/febrero para EBGR — confirmado, no es un problema de búsqueda.

**Para decidir con el equipo:** si esos ~13 archivos de febrero van a
producción tal cual, o si conviene reprocesarlos con el mismo fix antes
(mismo patrón ya usado hoy para enero — bajo esfuerzo, scripts ya
existen en `tst_files/reprocessing/`).

---

## Resumen para la reunión

| # | Ítem | Bloqueante para julio | Acción |
|---|---|---|---|
| 1 | Tipo de cambio / ARDEF / IAR desactualizados | Parcial — ARDEF/IAR refrescados 2026-08-12, FX sigue ~9 días de atraso | Refrescar FX antes de procesar fechas recientes; decidir dueño+cadencia |
| 2 | ~~Flujo automático sin probar desde hace 6+ semanas~~ | **RESUELTO 2026-08-13** | Smoke test end-to-end con archivo real de EBGR hecho y validado — ver sección 2 arriba |
| 3 | SFTP — mecanismo desconocido | **Sí, si es el canal real de EBGR** | Confirmar con el equipo antes de asumir que "cargar archivos" es solo soltarlos en landing |
| 4 | Febrero 2026 sin reprocesar | No bloqueante, pero pendiente de decisión | Decidir alcance antes de dar EBGR por "al día" |
