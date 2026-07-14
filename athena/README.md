# Athena

Consultas SQL sobre los Parquets finales en `s3-operational` y `s3-analytics`,
vía el catálogo de Glue (crawlers en `glue/crawlers/`, databases en
`glue/databases/`) — un database por combinación `{cliente}_{marca}`, ej.
`itl_0004_itx_dev_02_glue_database_operational_ebgr_visa`.

## Estado

Pendiente de configuración y validación (ver `.claude/memory/pending.md`).
Los crawlers de Glue ya pueblan el catálogo; falta definir workgroup y
bucket de resultados dedicados para este proyecto.

## Nota

`catalogs.json` y `workgroups.json` en esta carpeta son ejemplos del
scaffolding inicial (nomenclatura `itx_reference`/`ebgr_visa_staging`) — no
hay script de sincronización para Athena (a diferencia de Lambdas/Glue/
DynamoDB/Step Functions, ver `scripts/`), así que no reflejan el estado
real en AWS. Bucket de resultados: `aws-athena-query-results-{account}-{region}`
(auto-generado por AWS).
