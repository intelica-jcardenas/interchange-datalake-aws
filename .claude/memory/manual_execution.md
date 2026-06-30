# Ejecución manual de pasos del pipeline (debugging)

Contexto: para depurar el pipeline es mucho más rápido ejecutar cada paso a mano que volver a subir el archivo al S3 landing y esperar que el router + Step Functions arranquen todo desde cero.

---

## Prerequisito — autenticación AWS

```powershell
aws sso login --profile itx-dev
$env:AWS_PROFILE = "itx-dev"   # opcional, evita pasar --profile en cada comando
```

---

## Organización de tst_files/ (convención)

`tst_files/` está en `.gitignore` — es el scratch space local de debugging, no se versiona. Todo archivo nuevo debe ir en una carpeta, no suelto en la raíz.

| Carpeta | Contenido |
|---------|-----------|
| `glue_args/` | `.txt` con argumentos + `.json` generado por `generate_glue_args.py`, por job |
| `reprocessing/` | Scripts de reproceso masivo (`reprocess_vi_*.py`) + sus logs `.jsonl` |
| `debug_scripts/` | Scripts de debug/validación reutilizables (escanear schemas, comparar outputs) |
| `reference_data/` | Parquets/CSVs de tablas de referencia de `s3-reference` para inspección local |
| `reports/` | Outputs de `glue-test-1` y comparativos contra legacy |
| `payloads/` | Payloads de Lambda para reprocesos manuales (mc-store, vi-store) |

Cuando un gotcha pasa a **RESUELTO Y VALIDADO**, borrar los archivos de `tst_files/` que sirvieron solo para esa investigación. Mantener: scripts genéricos reutilizables (`generate_glue_args.py`, `scan_nulltype_columns.py`, `compare_get_transaction.py`) y datos de referencia activos (`reference_data/`).

---

## Flujo de trabajo para Glue Jobs

### 1. Preparar argumentos

Pegar clave/valor en líneas alternas en `tst_files/glue_args/<job>-run-test.txt`:
```
--content_hash
D44C4427AED04C1E078AA86B275060FA
--client_id
EBGR
```

### 2. Generar JSON

```powershell
python tst_files/glue_args/generate_glue_args.py
# genera tst_files/glue_args/vi-calculate-run-args.json

python tst_files/glue_args/generate_glue_args.py mi_args.txt mi_args.json  # paths custom
```

### 3. Lanzar job

```powershell
aws glue start-job-run `
  --profile itx-dev `
  --job-name itl-0004-itx-dev-intchg-02-glue-vi-calculate `
  --arguments "file://tst_files/glue_args/vi-calculate-run-args.json"
# devuelve: { "JobRunId": "jr_..." }
```

### 4. Verificar estado

```powershell
aws glue get-job-run `
  --profile itx-dev `
  --job-name itl-0004-itx-dev-intchg-02-glue-vi-calculate `
  --run-id jr_XXXX `
  --query "JobRun.{State:JobRunState,Error:ErrorMessage,Start:StartedOn}" `
  --output table
```

---

## Nombres reales de los Glue Jobs

| Job | Nombre AWS |
|-----|-----------|
| vi-calculate | `itl-0004-itx-dev-intchg-02-glue-vi-calculate` |
| vi-interchange | `itl-0004-itx-dev-intchg-02-glue-vi-interchange` |
| mc-calculate | `itl-0004-itx-dev-intchg-02-glue-mc-calculate` |
| mc-interchange | `itl-0004-itx-dev-intchg-02-glue-mc-interchange` |
| reporting | `itl-0004-itx-dev-intchg-02-glue-test-1` |
| vi-data-quality | `itl-0004-itx-dev-intchg-02-glue-test-3` |

---

## Subir script Glue a S3

`sync-glue.ps1` solo descarga (AWS → repo). Para subir un script editado localmente:

```powershell
# El ScriptLocation real está en glue/scripts/<marca>/<job>/config.json → Job.Command.ScriptLocation
aws s3 cp glue/scripts/visa/calculate/calculate.py `
  s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/calculate.py `
  --profile itx-dev
```

El siguiente `start-job-run` usa automáticamente la versión recién subida — sin pasos adicionales.

Rutas S3 reales por job:
| Job | ScriptLocation S3 |
|-----|-------------------|
| vi-calculate | `glue/scripts/visa/calculate.py` |
| vi-interchange | `glue/scripts/visa/interchange.py` |
| mc-calculate | `glue/scripts/mastercard/calculate.py` |
| mc-interchange | `glue/scripts/mastercard/interchange.py` |
| reporting (glue-test-1) | `glue/scripts/report/get_transaction.py` |

Todos bajo `s3://itl-0004-itx-dev-intchg-02-s3-reference/`.

---

## Crawlers

### Lanzar y verificar

```powershell
aws glue start-crawler --profile itx-dev --name itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa
# sin output = arrancó correctamente

aws glue get-crawler --profile itx-dev `
  --name itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa `
  --query "Crawler.{State:State,LastStatus:LastCrawl.Status,Start:LastCrawl.StartTime}" `
  --output table
# Estados: READY (idle), RUNNING, STOPPING
```

### Nombres de crawlers

| Crawler | Nombre AWS |
|---------|-----------|
| Staging EBGR VISA | `itl_0004_itx_dev_02_glue_crawler_staging_ebgr_visa` |
| Operational EBGR VISA | `itl_0004_itx_dev_02_glue_crawler_operational_ebgr_visa` |

---

## Lambdas (ejecución directa)

```powershell
# Sync (espera resultado):
aws lambda invoke `
  --profile itx-dev `
  --function-name itl-0004-itx-dev-intchg-02-lmbd-vi-store `
  --payload "file://tst_files/payloads/payload.json" `
  --cli-binary-format raw-in-base64-out `
  tst_files/payloads/response.json

# Async (fire-and-forget):
aws lambda invoke `
  --profile itx-dev `
  --invocation-type Event `
  --function-name itl-0004-itx-dev-intchg-02-lmbd-vi-transform `
  --payload "file://tst_files/payloads/payload.json" `
  --cli-binary-format raw-in-base64-out `
  tst_files/payloads/response.json

# mc-store necesita timeout extendido (hasta 900s):
aws lambda invoke --profile itx-dev `
  --function-name itl-0004-itx-dev-intchg-02-lmbd-mc-store `
  --payload file://tst_files/payloads/payload.json `
  --cli-binary-format raw-in-base64-out --cli-read-timeout 960 `
  tst_files/payloads/response.json
```

---

## Verificar S3

```powershell
# Listar staging de un cliente/marca:
aws s3 ls s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/VISA/ --profile itx-dev

# Verificar Parquet de un file_id concreto:
aws s3 ls "s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/VISA/400_baseii_cal_drafts/file_type=IN/date=2026-01-03/" --profile itx-dev
```

---

## Verificar tablas en Glue catalog

```powershell
aws glue get-tables --profile itx-dev `
  --database-name itl_0004_itx_dev_02_glue_database_staging_ebgr_visa `
  --query "TableList[].{Name:Name,Updated:UpdateTime}" `
  --output table

# Verificar columnas específicas:
aws glue get-table --profile itx-dev `
  --database-name itl_0004_itx_dev_02_glue_database_operational_ebgr_visa `
  --name baseii_drafts `
  --query "Table.StorageDescriptor.Columns[?Name=='business_transaction_cycle']"
```

---

## DynamoDB — consultar file_control

```powershell
# Por file_id (PK real de la tabla):
aws dynamodb get-item --profile itx-dev `
  --table-name itl-0004-itx-dev-dynamo-file_control-02 `
  --key '{"file_id": {"S": "<file_id>"}}' `
  --query "Item.{status:control_status.S, store:store_result.S, error:error_message.S}" --output json

# Por content_hash (NO es el PK — requiere scan):
aws dynamodb scan --profile itx-dev `
  --table-name itl-0004-itx-dev-dynamo-file_control-02 `
  --filter-expression "content_hash = :h" `
  --expression-attribute-values '{":h": {"S": "<hash>"}}' `
  --query "Items[0].{file_id:file_id.S, status:control_status.S}" --output json

# Por cliente y rango de fechas:
aws dynamodb scan --profile itx-dev `
  --table-name itl-0004-itx-dev-dynamo-file_control-02 `
  --filter-expression "client_id = :c AND file_processing_date BETWEEN :d1 AND :d2" `
  --expression-attribute-values file://tmp_filter.json `
  --query "Items[].{file_id:file_id.S, processing_date:file_processing_date.S, status:control_status.S, brand:brand_id.S, file_type:file_type.S, error:error_message.S}" `
  --output json
```

**Campos correctos en DynamoDB:** `brand_id` (no `brand`), `file_processing_date` (no `file_date`).
**`file_id` y `content_hash` pueden ser distintos** — DynamoDB usa `file_id` como PK.

---

## Reproceso manual lmbd-vi-store

`store_result` en DynamoDB contiene `outputs[].cln_s3_key` por `output_type` (BASEII, VSS_110/120/130/140).

```json
{
  "client_id": "EBGR",
  "file_id": "<file_id>",
  "brand": "VISA",
  "file_type": "IN",
  "file_date": "<YYYY-MM-DD>",
  "content_hash": "<content_hash>",
  "outputs": [
    {
      "output_type": "BASEII",
      "s3_key": "EBGR/VISA/300_baseii_cln_drafts/file_type=IN/date=<YYYY-MM-DD>/<content_hash>.parquet"
    }
  ]
}
```

Puede incluir múltiples `output_type` en un solo payload — el handler itera internamente. Para reproceso masivo usar `tst_files/reprocessing/reprocess_vi_store.py` (ThreadPoolExecutor, max_workers=20).

---

## Reproceso manual lmbd-mc-store

```powershell
# Listar bloques CLN de un MTI:
aws s3 ls "s3://itl-0004-itx-dev-intchg-02-s3-staging/SBSA/MC/400_IPM_1240_CLN/file_type=IN/date=2026-01-03/" `
  --profile itx-dev
```

Payload — un item por bloque CLN de cada MTI:
```json
{
  "client_id": "SBSA",
  "file_id": "<file_id>",
  "brand": "MC",
  "file_type": "IN",
  "file_date": "<YYYY-MM-DD>",
  "content_hash": "<content_hash>",
  "outputs": [
    {"mti": "1240", "s3_key": "SBSA/MC/400_IPM_1240_CLN/file_type=IN/date=<YYYY-MM-DD>/<archivo>.parquet"},
    {"mti": "1442", "s3_key": "SBSA/MC/400_IPM_1442_CLN/..."},
    {"mti": "1644", "s3_key": "SBSA/MC/400_IPM_1644_CLN/..."},
    {"mti": "1740", "s3_key": "SBSA/MC/400_IPM_1740_CLN/..."}
  ]
}
```

**mc-store NO actualiza DynamoDB** (lo hace el Step Function). Tras un reproceso manual, actualizar `file_control-02` con boto3:
```python
table.update_item(
    Key={'file_id': '<file_id>'},
    UpdateExpression='SET store_result = :sr, process_finish_ts = :ts, error_message = :null',
    ExpressionAttributeValues={':sr': json.dumps(store_result), ':ts': now, ':null': None}
)
# IMPORTANTE: SET error_message = :null (None), NUNCA usar REMOVE
```

---

## glue update-job — UTF-8 sin BOM (PowerShell 5.1)

PowerShell 5.1 escribe UTF-16 con BOM por defecto. `aws glue update-job --job-update file://...` falla silenciosamente con ese encoding. Usar `System.Text.UTF8Encoding($false)`:

```powershell
$job = (aws glue get-job --profile itx-dev --job-name <nombre-job> | ConvertFrom-Json).Job
$payload = @{
    Role = $job.Role
    Command = @{ Name = $job.Command.Name; ScriptLocation = $job.Command.ScriptLocation; PythonVersion = $job.Command.PythonVersion }
    GlueVersion = $job.GlueVersion
    DefaultArguments = @{
        "--operational_bucket" = "itl-0004-itx-dev-intchg-02-s3-operational"
        # ... resto de args
    }
}
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText("$PWD\tmp_update_job.json", ($payload | ConvertTo-Json -Depth 5), $utf8NoBom)
aws glue update-job --profile itx-dev --job-name <nombre-job> --job-update file://tmp_update_job.json
Remove-Item tmp_update_job.json
```

El payload de `update-job` requiere `Role`, `Command` y `GlueVersion` además de `DefaultArguments` — sin ellos AWS retorna error. Tras el update, hacer sync (`.\scripts\sync-glue.ps1 -Job <nombre>`) para reflejar los cambios en `args.json`.

---

## Scripts de reproceso masivo (referencia rápida)

Todos en `tst_files/reprocessing/`. Patrón común: scan DynamoDB `file_control-02` → filtrar por cliente/estado/fechas → lanzar jobs/lambdas con concurrencia controlada → log `.jsonl`.

| Script | Qué hace |
|--------|----------|
| `reprocess_vi_calculate.py` | Relanza `glue-vi-calculate` (MaxConcurrentRuns=50, límite efectivo 45) |
| `reprocess_vi_store.py` | Reinvoca `lmbd-vi-store` todos los output_types (ThreadPoolExecutor max_workers=20) |
| `reprocess_vi_vss_store.py` | Igual que anterior pero solo VSS_110/120/130/140 |
| `reprocess_vi_interchange.py` | Relanza `glue-vi-interchange` (listo, no ejecutado aún — ver `pending.md`) |

**Escanear NullType en S3 sin descargar archivos:** `tst_files/debug_scripts/scan_nulltype_columns.py` — lee solo el footer Parquet vía `pq.ParquetFile(...).schema_arrow`. Ajustar `BUCKET`/`PREFIX`.

**Comparar reporte vs legacy:** `tst_files/debug_scripts/compare_get_transaction_aggregated.py` — lee Parquet local en chunks + queries `GROUP BY` contra PRD via psycopg2 (credenciales en memoria `prd-db-credentials.md`).
