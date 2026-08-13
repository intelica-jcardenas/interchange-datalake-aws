# Layer — `itl-0004-itx-dev-intchg-02-rules-refresh-openpyxl`

Layer con `openpyxl`+`et_xmlfile` (lectura de excels), usado únicamente por
`lmbd-rules-refresh` (hoy desplegado sobre `itl-0004-itx-dev-intchg-02-lmbd-test-1`,
ver `lambdas/rules-refresh/README.md`) para leer los excels de reglas de
interchange (`visa_rules`/`mc_rules`). Antes de este layer, ambas
dependencias iban empaquetadas junto al código del Lambda (único caso en
el repo sin layer propio) — resuelto 2026-08-13.

`layer.zip` es el paquete real subido a AWS (estructura `python/` en la
raíz, como requiere Lambda). Ambas librerías son 100% Python puro (sin
extensiones compiladas), por eso el layer funciona igual sin importar el
runtime/arquitectura del Lambda que lo use.

**ARN publicado:** `arn:aws:lambda:eu-south-2:861276092327:layer:itl-0004-itx-dev-intchg-02-rules-refresh-openpyxl:1`

Regenerar si hace falta actualizar versión de `openpyxl`/`et_xmlfile`:
```powershell
# 1. pip install openpyxl et_xmlfile -t <carpeta>/python/
# 2. zip -r layer.zip python/ (o el equivalente en PowerShell/Python zipfile)
# 3. aws s3 cp layer.zip s3://itl-0004-itx-dev-intchg-02-s3-reference/layers/rules-refresh-openpyxl/layer.zip
# 4. aws lambda publish-layer-version --layer-name itl-0004-itx-dev-intchg-02-rules-refresh-openpyxl \
#      --content S3Bucket=itl-0004-itx-dev-intchg-02-s3-reference,S3Key=layers/rules-refresh-openpyxl/layer.zip \
#      --compatible-runtimes python3.11
# 5. aws lambda update-function-configuration --function-name <lambda> --layers <arn-pandas-pyarrow> <nuevo-arn>
```
