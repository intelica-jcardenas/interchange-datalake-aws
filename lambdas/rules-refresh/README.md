# `itl-0004-itx-dev-intchg-02-lmbd-rules-refresh` (desplegado sobre `lmbd-test-1`, nombre definitivo pendiente)

Automatiza el refresco de `visa_rules/data.parquet` y `mc_rules/data.parquet`
en `s3-reference` a partir de los excels de reglas de interchange que el
negocio sube periódicamente (hoy es un proceso manual — ver
`tst_files/interchange_rules/build_and_compare_rules.py` y la memoria de
usuario `interchange_rules_excel_refresh.md`).

Reemplaza en parte al proceso legacy (`InterchangeRules.py` /
`exec_master_interchage.py`, que leía estos mismos excels desde
`Intelica/INTERCHANGE_RULES/{VISA,MASTERCARD}/...` e insertaba a Postgres).

## Trigger propuesto

Evento S3 `ObjectCreated:*` en `s3-reference`, prefijo
`interchange_rules/`. **Importante:** configurar el filtro de prefijo
exactamente como `interchange_rules/` (no más angosto) — el propio Lambda
distingue la marca por el subprefijo (`VISA/` o `MASTERCARD/`) y evita
reprocesar sus propios archivos de salida (`_archive/`, `_rejected/`, ver
`_is_admin_subpath()`). Se recomienda además un filtro de sufijo `.xlsx`
en la notificación S3 (el handler también lo valida, pero evita invocaciones
innecesarias).

## Estructura de carpetas en `s3-reference`

```
interchange_rules/
├── VISA/
│   ├── (excels subidos por el negocio, cualquier nombre — ej. "VISA Reglas Intercambio V38.xlsx")
│   ├── _archive/{timestamp}_{filename}      ← excels procesados con éxito
│   └── _rejected/{timestamp}_{filename}     ← excels que fallaron validación
└── MASTERCARD/
    ├── (excels subidos por el negocio)
    ├── _archive/{timestamp}_{filename}
    └── _rejected/{timestamp}_{filename}

visa_rules/
├── data.parquet                              ← parquet productivo (consumido por glue-vi-interchange)
└── history/data_{timestamp}.parquet          ← backup del data.parquet anterior, uno por refresh exitoso

mc_rules/
├── data.parquet
└── history/data_{timestamp}.parquet
```

## Flujo

1. Detecta la marca por el subprefijo del key.
2. Descarga el excel completo (son archivos chicos, a diferencia de los
   archivos transaccionales del pipeline — no hace falta streaming).
3. Valida estructura — misma lógica que `read_rules_visa`/`read_rules_mc`
   de legacy (ya probada contra los excels reales V37/V23), más 3 guardas
   nuevas que el proceso manual no tenía:
   - Nulls en columnas requeridas → falla dura (antes solo se logueaba).
   - Un valor numérico que no parsea (ej. `"1.75%"` en vez de `0.0175`) →
     falla dura. Para `RATE_VARIABLE` de MC (columna que ni siquiera se
     castea/almacena en esta etapa) solo aplica sobre reglas **todavía
     vigentes** — el excel real V23 ya trae 28 filas de Bahamas con este
     problema en reglas YA VENCIDAS (ver `gotchas.md`); sin ese matiz el
     refresh rechazaría el excel real de producción siempre.
   - Menos del 50% de las filas del `data.parquet` actual (`RULES_MIN_ROW_RATIO`)
     → falla dura — guarda barata contra "subieron el sheet equivocado".
4. Si falla: mueve el excel a `_rejected/`, loguea `FAILED`.
   **No toca el `data.parquet` productivo.**
5. Si valida OK: calcula el diff contra el parquet actual (altas/bajas/
   columnas con valores distintos), respalda el actual a `history/`,
   publica el nuevo `data.parquet`, mueve el excel a `_archive/` y loguea
   `SUCCESS` con el resumen del diff.

**Publish automático** (decisión explícita del usuario, 2026-08-03): si el
excel pasa la validación estructural, se publica solo — no espera
aprobación manual. La validación estructural no puede detectar errores de
*contenido* (ej. un código de región mal cargado, como pasó con Belarús —
ver `decisions.md`) — el diff que queda logueado en CloudWatch (sin tabla
de auditoría en DynamoDB, decisión explícita — ver más abajo) es la
principal herramienta para detectar ese tipo de problema después, junto
con los comparativos contra legacy que ya se corren para cada refresh de
reglas.

## Validación local ya hecha (2026-08-03)

`_build_visa_rules()`/`_build_mc_rules()` se corrieron contra los 2 excels
reales del repo (`tst_files/interchange_rules/VISA Reglas Intercambio
V37.xlsx`, `MASTERCARD Reglas Intercambio V23.xlsx`) y su resultado se
comparó (`pd.testing.assert_frame_equal`, `check_dtype=False`) contra el
parquet ya generado por el prototipo `build_and_compare_rules.py` —
**idéntico fila a fila en ambas marcas**. También se confirmó que el ajuste
de `RATE_VARIABLE`/reglas vigentes evita que el excel V23 real (que SÍ
tiene el problema de Bahamas) sea rechazado. No se probó contra AWS real
(S3) — eso requiere que la infraestructura exista primero.

## Rollback manual

No hay rollback automático — si un refresh ya publicado resulta tener un
problema de contenido, restaurar es una copia S3 deliberada desde
`{visa_rules|mc_rules}/history/data_{timestamp}.parquet` de vuelta a
`data.parquet` (mismo nivel de cuidado que un commit de git — no se hace
sin pedido explícito).

## Sin tabla de auditoría en DynamoDB (decisión explícita, 2026-08-10)

Este Lambda **no** registra sus corridas en ninguna tabla DynamoDB — se
evaluó (`rules_control`, schema llegó a redactarse) y se descartó: es un
proceso chico y de baja frecuencia (el negocio sube un excel nuevo cada
tanto, no un flujo transaccional de alto volumen como `file_control`), no
justifica una tabla propia. El resultado de cada refresh (excel leído, diff
contra el parquet actual, éxito o motivo de rechazo) queda en CloudWatch —
suficiente para este volumen. Si en el futuro se necesita auditoría
estructurada (dashboards, alertas, consultas históricas), evaluarlo de
nuevo en ese momento, no antes.

## Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `S3_BUCKET_REFERENCE` | Bucket `s3-reference` (incoming, parquet, history, archive) |
| `RULES_MIN_ROW_RATIO` | Ratio mínimo filas_nuevas/filas_actuales antes de rechazar (default `0.5`) |

## Estado real (actualizado 2026-08-13)

Desplegado y en producción desde 2026-08-10, corriendo sobre infraestructura
prestada (`itl-0004-itx-dev-intchg-02-lmbd-test-1`, rol
`itl-0004-itx-dev-intchg-02-lmbd-vi-role`) — ver detalle completo en
`.claude/memory/decisions.md` (proyecto). Notificación S3 activa en
`s3-reference` (prefijo `interchange_rules/`, sufijos `.xlsx`/`.xls`),
auto-publica sin aprobación manual.

**Dependencias:** `openpyxl`/`et_xmlfile` ya NO van empaquetadas junto al
código — resuelto 2026-08-13 con un layer dedicado
(`layers/rules-refresh-openpyxl/`, ver su propio README), mismo patrón que
todos los demás Lambdas del proyecto. `src/` local solo tiene `handler.py`.

## Pendiente

- [ ] **Lambda/rol propios** — sigue sobre `lmbd-test-1`/`lmbd-vi-role`
  prestados. Renombrar o recrear con nombre definitivo
  (`itl-0004-itx-dev-intchg-02-lmbd-rules-refresh`) y rol IAM dedicado
  (permisos: `s3:GetObject`/`PutObject`/`CopyObject`/`DeleteObject` sobre
  `s3-reference`, scoped a `interchange_rules/*`, `visa_rules/*`,
  `mc_rules/*`).
- [ ] Decidir si se crea una lifecycle policy en `history/` (borrado
  automático tras N días) o se deja crecer indefinidamente.
