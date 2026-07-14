# IAM Roles

11 roles con permisos granulares (S3, DynamoDB, Step Functions, Glue,
Lambda invoke). Patrón de nombres:
`itl-0004-itx-{env}-intchg-02-lmbd-{marca}-role` para Lambdas — ej.
`itl-0004-itx-dev-intchg-02-lmbd-mc-role`; los Glue jobs comparten
`itl-0004-itx-dev-intchg-02-glue-role` (jobs de negocio) o
`itl-0004-itx-dev-intchg-02-glue-test-role` (data quality/reportes,
heredado de cuando esos jobs se llamaban `glue-test-N`).

## Pendiente (ver `.claude/memory/pending.md`)

- `lmbd-vi-extract` comparte rol con el router — falta un
  `itx-lambda-extract-role` propio con permisos mínimos.
- El crawler Mastercard no tiene rol propio.

## Nota

`roles/*.json` en esta carpeta son exports del scaffolding inicial
(nomenclatura `itx-lambda-router-role`, etc.) — no hay script de
sincronización para IAM (a diferencia de Lambdas/Glue/DynamoDB/Step
Functions), así que no reflejan los roles reales actuales.
