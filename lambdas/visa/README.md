# Lambdas — Visa

Etapas del pipeline Visa (IN/OUT) más los dos módulos independientes
(`ardef`, `exchange-rates`). Cada subcarpeta sigue la estructura estándar
`src/handler.py` + `config.json` + `env-vars.json`. El detalle de cada etapa
vive en el docstring de módulo de su `handler.py` (ya documentado, ver skill
`itx-document-script`) — esta tabla es solo el mapa de navegación.

| Carpeta | Lambda real | Rol |
|---------|-------------|-----|
| `transform/` | `itl-0004-itx-dev-intchg-02-lmbd-vi-transform` | 1ra etapa: texto plano latin-1 ancho fijo → Parquet (BASEII/SMS/VSS) |
| `extract/` | `itl-0004-itx-dev-intchg-02-lmbd-vi-extract` | 2da etapa: extrae campos por posición fija según DynamoDB `visa_fields` |
| `clean/` | `itl-0004-itx-dev-intchg-02-lmbd-vi-clean` | 3ra etapa: castea/normaliza tipos y fechas (`!YDDD`, `!MMDD`, etc.) |
| `store/` | `itl-0004-itx-dev-intchg-02-lmbd-vi-store` | Última etapa: consolida CLN+CAL+ITX y escribe el Parquet final a operational |
| `ardef/` | `itl-0004-itx-dev-intchg-02-lmbd-vi-ardef` | Motor de reglas ARDEF (rangos de BINes/fees) — invocado directo por el router, sin Step Functions (`direction=ARDEF`) |
| `exchange-rates/` | `itl-0004-itx-dev-intchg-02-lmbd-vi-exchange-rates` | Scraping de tipos de cambio. **Imagen Docker** (`PackageType=Image`) — no tiene código fuente sincronizable en este repo; `handler.py` es un placeholder vacío |

`calculate`/`interchange` (Glue, no Lambda) vienen después de `clean` — ver
`glue/README.md`.
