# DynamoDB — Tablas del proyecto

Pipeline configuration-driven: la lógica de clasificación, mapeo y
validación vive en estas tablas, no hardcodeada en el código. Patrón de
nombres: `itl-0004-itx-{env}-dynamo-{tabla}-02`.

| Tabla | PK | Propósito |
|-------|----|-----------| 
| `file_control` | `file_id` | Tracking de archivos procesados (~55 items) |
| `file_pattern` | `pattern_id` | Patrones regex para clasificar tipo de archivo; incluye `file_block`/`interpreter_fix` para Mastercard |
| `visa_fields` | `type_record` (HASH) + `column_name` (RANGE) | Definición de campos Visa por tipo de archivo (~430 items); GSI `type-record-index` usado por `lmbd-vi-clean` |
| `mastercard_fields` | `type_record` (DE o PDS) | Definición de Data Elements y PDS Mastercard por MTI |
| `client` | `client_id` | Catálogo de clientes; incluye encoding Mastercard por dirección (`file_mc_encoding_in`/`_out`), BINs, monedas |

`schemas/` y `items/` se mantienen sincronizados con AWS vía
`scripts/sync-dynamodb.ps1` (`items/` excluye `file_control`, es data
operacional, no configuración).
