# ITX AWS Pipeline — interchange-datalake-aws

Pipeline serverless para procesamiento de archivos de interchange Visa y Mastercard.
Transforma archivos raw (Visa: texto plano latin-1; Mastercard: IPM/ISO-8583, latin-1 o EBCDIC)
en datos estructurados Parquet sobre una arquitectura Data Lake en AWS.

**Organización:** Intelica IT | **Runtime:** Python 3.11 | **Región:** eu-south-2

---

## Arquitectura

```
S3 Landing
    |  [s3:ObjectCreated:*]
    v
itl-0004-itx-{env}-intchg-02-lmbd-unzip          (si es ZIP)
    |
    v
itl-0004-itx-{env}-intchg-02-lmbd-router
    |  [clasifica via DynamoDB, inicia Step Function]
    |
    +--[Visa]----> itl-0004-itx-dev-intchg-02-sfn-vi
    |                   |
    |              lmbd-vi-transform      Texto plano latin-1 -> Parquet (BASE II, SMS, VSS)
    |              lmbd-vi-extract        Extraccion de campos por posicion fija
    |              lmbd-vi-clean          Normalizacion y formateo (via visa_fields DynamoDB)
    |              lmbd-vi-ardef          Carga tabla ARDEF (BINes y tarifas de la marca)
    |              glue-vi-calculate      Campos derivados para tarificacion
    |              glue-vi-interchange    Asignacion de tarifas de interchange + DQ vs VSS
    |              lmbd-vi-store          Consolida CLN+CAL+ITX -> S3 Operational
    |
    +--[MC]------> itl-0004-itx-dev-intchg-02-sfn-mc
                        |
                   lmbd-mc-interpreter    IPM/ISO-8583 -> Parquets por MTI (paso exclusivo MC)
                   lmbd-mc-transform      Estructuracion de DEs y PDS por MTI
                   lmbd-mc-extract        Extraccion de campos (via mastercard_fields DynamoDB)
                   lmbd-mc-clean          Normalizacion y formateo de campos
                   lmbd-mc-iar            Carga tabla IAR (BINes y tarifas de la marca)
                   glue-mc-calculate      Campos derivados para tarificacion
                   glue-mc-interchange    Asignacion de tarifas de interchange por transaccion (MTIs 1240/1442)
                   lmbd-mc-store          Consolida CLN+CAL+ITX por MTI -> S3 Operational
    |
    v
itl-0004-itx-{env}-intchg-02-lmbd-archive-file   Archiva original -> S3 Archive
    |
    v
Athena (*)                                        Consultas SQL sobre datos finales
```

`(*)` Athena: configuracion de crawlers, workgroups y tablas pendiente de validacion

---

## Recursos AWS

### Lambdas

| Nombre (env=dev) | Rol |
|------------------|-----|
| `itl-0004-itx-dev-intchg-02-lmbd-router` | Clasifica archivos y dispara el Step Function |
| `itl-0004-itx-dev-intchg-02-lmbd-unzip` | Descomprime ZIPs antes del router |
| `itl-0004-itx-dev-intchg-02-lmbd-archive-file` | Archiva el archivo original post-proceso |
| `itl-0004-itx-dev-intchg-02-lmbd-vi-transform` | Visa: agrupa records del archivo plano latin-1 (BASE II, SMS, VSS) |
| `itl-0004-itx-dev-intchg-02-lmbd-vi-extract` | Visa: extraccion de campos por posicion fija segun manuales |
| `itl-0004-itx-dev-intchg-02-lmbd-vi-clean` | Visa: normalizacion y formateo de campos (via visa_fields DynamoDB) |
| `itl-0004-itx-dev-intchg-02-lmbd-vi-store` | Visa: consolida clean+calculate+interchange -> S3 Operational |
| `itl-0004-itx-dev-intchg-02-lmbd-vi-ardef` | Visa: motor de reglas ARDEF (rangos de BINes y fees) |
| `itl-0004-itx-dev-intchg-02-lmbd-vi-exchange-rates` | Visa: conversion de moneda |
| `itl-0004-itx-dev-intchg-02-lmbd-mc-interpreter` | MC: parser IPM/ISO-8583 — traduce archivo a Parquets por MTI (paso exclusivo MC) |
| `itl-0004-itx-dev-intchg-02-lmbd-mc-transform` | MC: estructuracion de DEs y PDS por MTI (preparacion previa al extract) |
| `itl-0004-itx-dev-intchg-02-lmbd-mc-extract` | MC: alinea schema TRA contra DynamoDB, renombra columnas a nombres extract estandarizados, escribe capa EXT |
| `itl-0004-itx-dev-intchg-02-lmbd-mc-clean` | MC: castea y normaliza columnas segun dtype DynamoDB, aplica conversion de moneda, escribe capa CLN |
| `itl-0004-itx-dev-intchg-02-lmbd-mc-iar` | MC: motor de reglas IAR (rangos de BINes y fees) |
| `itl-0004-itx-dev-intchg-02-lmbd-mc-store` | MC: consolida CLN+CAL+ITX por MTI -> S3 Operational |
| `itl-0004-itx-dev-intchg-02-lmbd-mc-exchange-rates` | MC: conversion de moneda |

### Glue Jobs

| Nombre (env=dev) | Marca | Workers |
|------------------|-------|---------|
| `itl-0004-itx-dev-intchg-02-glue-vi-calculate` | Visa | G.1X × 2 |
| `itl-0004-itx-dev-intchg-02-glue-vi-interchange` | Visa | G.2X × 4 |
| `itl-0004-itx-dev-intchg-02-glue-mc-calculate` | MC | G.1X × 2 |
| `itl-0004-itx-dev-intchg-02-glue-mc-interchange` | MC | G.1X × 2 |
| `itl-0004-itx-dev-intchg-02-glue-get-transaction` (reporte de transacciones) | Visa/MC | G.1X × 2 |
| `itl-0004-itx-dev-intchg-02-glue-exchange-rates` (enriquece tipos de cambio) | — | G.1X × 2 |
| `itl-0004-itx-dev-intchg-02-glue-vi-data-quality` | Visa | G.1X × 2 |
| `itl-0004-itx-dev-intchg-02-glue-mc-data-quality` | MC | G.1X × 2 |
| `itl-0004-itx-dev-intchg-02-glue-scheme-fee` (cuotas) | Visa/MC | G.1X × 2 |

### Step Functions

| Nombre | Marca | Estado |
|--------|-------|--------|
| `itl-0004-itx-dev-intchg-02-sfn-vi` | Visa | Funcional, validado |
| `itl-0004-itx-dev-intchg-02-sfn-mc` | Mastercard | Funcional, validado (comparativos EBGR + SBSA enero 2026) |

### S3 Buckets

| Nombre (env=dev) | Capa |
|------------------|------|
| `itl-0004-itx-dev-intchg-02-s3-landing` | Entrada — archivos raw |
| `itl-0004-itx-dev-intchg-02-s3-staging` | Intermedio — Parquets de proceso |
| `itl-0004-itx-dev-intchg-02-s3-operational` | Salida — Parquets finales |
| `itl-0004-itx-dev-intchg-02-s3-archive` | Archivo — originales post-proceso |
| `itl-0004-itx-dev-intchg-02-s3-reference` | Referencia — ARDEF, IAR, tipos de cambio |

### DynamoDB

| Nombre (env=dev) | Proposito |
|------------------|-----------|
| `itl-0004-itx-dev-dynamo-file_control-02` | Tracking de archivos procesados |
| `itl-0004-itx-dev-dynamo-file_pattern-02` | Patrones regex para clasificacion; incluye config de bloqueo MC |
| `itl-0004-itx-dev-dynamo-visa_fields-02` | Definicion de campos Visa por tipo de archivo |
| `itl-0004-itx-dev-dynamo-mastercard_fields-02` | Definicion de DEs y PDS Mastercard por MTI |
| `itl-0004-itx-dev-dynamo-client-02` | Catalogo de clientes; incluye encoding MC por direccion |

---

## Estructura del Repositorio

```
interchange-datalake-aws/
├── lambdas/
│   ├── router/
│   ├── unzip/
│   ├── archive-file/
│   ├── visa/
│   │   ├── transform/
│   │   ├── extract/
│   │   ├── clean/
│   │   ├── store/
│   │   ├── ardef/
│   │   └── exchange-rates/
│   └── mastercard/
│       ├── transform/
│       ├── extract/
│       ├── clean/
│       ├── store/
│       ├── iar/
│       ├── interpreter/
│       └── exchange-rates/
├── step-functions/
│   ├── visa/asl.json
│   └── mastercard/asl.json
├── glue/scripts/
│   ├── visa/
│   └── mastercard/
├── dynamodb/schemas/
├── iam/roles/
├── s3/configs/
├── layers/itx-pandas-pyarrow/
├── athena/
├── scripts/
│   ├── sync-lambdas.ps1
│   └── sync-glue.ps1
├── infrastructure/
│   ├── deploy.sh
│   └── terraform/
├── .env.example
└── CLAUDE.md
```

---

## Deploy

```bash
git clone <repo>
cd interchange-datalake-aws
cp .env.example .env
# Editar .env con valores del ambiente destino
chmod +x infrastructure/deploy.sh
./infrastructure/deploy.sh
```

Alternativamente con Terraform:
```bash
cd infrastructure/terraform
terraform init && terraform plan && terraform apply
```

---

## Estado

| Componente | Estado |
|------------|--------|
| Pipeline Visa completo | Implementado y validado |
| Pipeline Mastercard completo | Implementado y validado end-to-end (comparativos EBGR + SBSA enero 2026) |
| Reporting (`get_transaction.py`) | Validado para ambas marcas |
| Scheme Fee (cuotas) | `--mode generate` validado end-to-end; `--mode read` pendiente |
| Athena (crawlers, workgroups, tablas) | Pendiente de configuracion y validacion |

Ver `CLAUDE.md` para documentacion tecnica detallada.
