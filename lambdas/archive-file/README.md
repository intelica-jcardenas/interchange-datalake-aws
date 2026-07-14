# `itl-0004-itx-dev-intchg-02-lmbd-archive-file`

Último paso del pipeline (Visa y Mastercard), corre siempre — éxito o
fallo. Comprime el archivo original de `s3-landing` a ZIP (streaming,
chunks de 8MB, nunca carga el archivo completo en RAM) y lo sube a
`s3-archive`, dejando el landing limpio. Borra el original solo después de
confirmar el ZIP en destino.

**Destino:** `{client_id}/originals/{brand}/{file_type}/{año}/{mes}/{filename}.zip`

## Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `S3_BUCKET_LANDING` | Bucket origen |
| `S3_BUCKET_ARCHIVE` | Bucket destino |
| `COMPRESS_CHUNK_SIZE_MB` | Tamaño de chunk al comprimir (default: 8MB) |
