# `itl-0004-itx-dev-intchg-02-lmbd-router`

Punto de entrada del pipeline. Trigger: S3 Event `ObjectCreated:*` en
`s3-landing`, path `{client_id}/{filename}`.

## Flujo

1. Extrae `client_id` del path.
2. Si es ZIP → delega a `lmbd-unzip` (async) y termina; cada archivo
   extraído re-dispara el router (paralelismo gratis).
3. Clasifica el archivo contra los patrones regex de DynamoDB
   `file_pattern` (por prioridad).
4. Extrae la fecha de negocio del contenido — lógica distinta por marca:
   header de 50 bytes (Visa), escaneo del trailer 1644/695 con descarga
   completa + `unblock_1014` para archivos bloqueados (Mastercard),
   formatos propios para IAR/ARDEF.
5. Calcula el MD5 (`content_hash`) en streaming (nunca carga el archivo
   completo) y detecta duplicados/nuevas versiones en `file_control`.
6. Registra el archivo en DynamoDB (`PENDING` → `PROCESSING`).
7. Despacha según `direction`/`brand`:
   - `ARDEF` → `lmbd-vi-ardef` (Lambda directa, sin Step Functions)
   - `IAR` → `lmbd-mc-iar` (Lambda directa, sin Step Functions)
   - `VISA` → Step Function `sfn-vi`
   - `MASTERCARD` → Step Function `sfn-mc`

## Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `S3_BUCKET_LANDING` | Bucket de landing |
| `DYNAMODB_TABLE_FILE_CONTROL` | Tabla de control de archivos |
| `DYNAMODB_TABLE_FILE_PATTERN` | Tabla de patrones de clasificación |
| `STEP_FUNCTION_VI_ARN` / `STEP_FUNCTION_MC_ARN` | ARNs de las Step Functions |
| `VISA_ARDEF_FUNCTION_NAME` / `MASTERCARD_IAR_FUNCTION_NAME` | Lambdas de reglas (invocación directa) |
| `UNZIP_FUNCTION_NAME` | Lambda de descompresión |
