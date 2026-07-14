# Step Functions

Dos orquestadores, uno por marca — el router despacha a cada uno según
`brand` (ver `lambdas/router/README.md`). ARDEF/IAR no pasan por Step
Functions (se invocan como Lambda directa, archivos livianos).

## `itl-0004-itx-dev-intchg-02-sfn-vi` (`visa/asl.json`)

```
lmbd-vi-transform → lmbd-vi-extract → lmbd-vi-clean
  → glue-vi-calculate → glue-vi-interchange
    → lmbd-vi-store → lmbd-archive-file
```

## `itl-0004-itx-dev-intchg-02-sfn-mc` (`mastercard/asl.json`)

```
lmbd-mc-interpreter → lmbd-mc-transform → lmbd-mc-extract → lmbd-mc-clean
  → glue-mc-calculate → [Wait 180s liberación ENI] → glue-mc-interchange
    → lmbd-mc-store → lmbd-archive-file
```

El `Wait` de 180s entre calculate e interchange existe porque
`glue-mc-calculate` corre con conexión VPC y AWS tarda en liberar las ENIs
asociadas — ver `.claude/memory/decisions.md`.

Ambas validadas end-to-end (comparativos EBGR + SBSA enero 2026).
Definiciones sincronizadas vía `scripts/sync-step-functions.ps1`.
