# Glue — Jobs, Crawlers, Databases

Patrón de nombres: `itl-0004-itx-{env}-intchg-02-glue-{marca}-{job}`. Glue
4.0. Cada job tiene su script en `scripts/{marca}/{job}/`, más
`config.json` (config real de AWS) y `args.json`
(`DefaultArguments`) — ambos gestionados por `scripts/sync-glue.ps1`/
`scripts/push-glue.ps1` (raíz del repo), no a mano.

## Jobs (9)

| Job | Marca | Workers | Propósito |
|-----|-------|---------|-----------|
| `glue-vi-calculate` | Visa | G.1X × 2 | Campos derivados para tarificación |
| `glue-vi-interchange` | Visa | G.2X × 4 | Asignación de tarifas + Data Quality vs VSS |
| `glue-mc-calculate` | MC | G.1X × 2 | Campos derivados para tarificación |
| `glue-mc-interchange` | MC | G.1X × 2 | Asignación de tarifas IAR (MTIs 1240/1442) |
| `glue-get-transaction` | Visa/MC | G.1X × 2 | Reporte de transacciones (`get_transaction.py`), un cliente por corrida |
| `glue-exchange-rates` | — | G.1X × 2 | Enriquece tipos de cambio con códigos numéricos (`format_exchange_rates.py`) — fuente oficial del pipeline |
| `glue-vi-data-quality` | Visa | G.1X × 2 | Data Quality Visa, aún no integrado a un Step Function |
| `glue-mc-data-quality` | MC | G.1X × 2 | Data Quality Mastercard, aún no integrado a un Step Function |
| `glue-scheme-fee` | Visa/MC | G.1X × 2 | Cuotas (scheme fees), `--mode generate`/`--mode read` |

## Crawlers y Databases

Un database por `{cliente}_{marca}` (ej.
`itl_0004_itx_dev_02_glue_database_operational_ebgr_visa`), poblado por su
crawler correspondiente. Inventario completo y estado de verificación →
`GLUE_CATALOG_CREATION.md`.
