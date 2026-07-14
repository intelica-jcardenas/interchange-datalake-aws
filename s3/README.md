# S3 — Buckets del Data Lake

5 buckets, uno por capa. Patrón de nombres:
`itl-0004-itx-{env}-intchg-02-s3-{tipo}`.

| Bucket | Capa | Contenido |
|--------|------|-----------|
| `s3-landing` | Entrada | Archivos raw, dispara el router vía S3 Event |
| `s3-staging` | Intermedio | Parquets de cada etapa (transform/extract/clean/calculate/interchange) |
| `s3-operational` | Salida | Parquets finales, consultables desde Athena |
| `s3-archive` | Archivo | Originales movidos post-procesamiento |
| `s3-reference` | Referencia | ARDEF, IAR, tipos de cambio (`exchange-rates-glue/`), country, currency, `mc_rules/` |

## Nota

`configs/itx-*-dev/` en esta carpeta son exports del scaffolding inicial
(nomenclatura vieja `itx-landing-dev`, etc.) — no hay script de
sincronización para S3 (a diferencia de Lambdas/Glue/DynamoDB/Step
Functions), así que no reflejan la configuración real actual.
