# Lambdas — Mastercard

Etapas del pipeline Mastercard (IN/OUT) más los dos módulos independientes
(`iar`, `exchange-rates`). Cada subcarpeta sigue la estructura estándar
`src/handler.py` + `config.json` + `env-vars.json`. El detalle de cada etapa
vive en el docstring de módulo de su `handler.py` (ya documentado, ver skill
`itx-document-script`) — esta tabla es solo el mapa de navegación.

| Carpeta | Lambda real | Rol |
|---------|-------------|-----|
| `interpreter/` | `itl-0004-itx-dev-intchg-02-lmbd-mc-interpreter` | 1ra etapa (exclusiva MC, sin equivalente Visa): archivo IPM binario ISO-8583 → Parquets por MTI |
| `transform/` | `itl-0004-itx-dev-intchg-02-lmbd-mc-transform` | 2da etapa: layout de DEs/PDS desde DynamoDB, expande subcampos y PDS (TLV) |
| `extract/` | `itl-0004-itx-dev-intchg-02-lmbd-mc-extract` | 3ra etapa: alinea schema y renombra columnas técnicas a nombres estandarizados |
| `clean/` | `itl-0004-itx-dev-intchg-02-lmbd-mc-clean` | 4ta etapa: castea tipos (`mastercard_fields`), conversión de moneda, schema Arrow explícito |
| `store/` | `itl-0004-itx-dev-intchg-02-lmbd-mc-store` | Última etapa: consolida CLN+CAL+ITX por llave `(file_id, file_idn, ref_id)` y escribe a operational |
| `iar/` | `itl-0004-itx-dev-intchg-02-lmbd-mc-iar` | Motor de reglas IAR (rangos de BINes/fees), equivalente a `ardef` de Visa — invocado directo por el router, sin Step Functions (`direction=IAR`) |
| `exchange-rates/` | `itl-0004-itx-dev-intchg-02-lmbd-mc-exchange-rates` | Scraping de tipos de cambio vía API pública del conversor Mastercard, con rotación de proxies (orquestador/worker/consolidador encadenado) |

`calculate`/`interchange` (Glue, no Lambda) vienen después de `clean` — ver
`glue/README.md`.
