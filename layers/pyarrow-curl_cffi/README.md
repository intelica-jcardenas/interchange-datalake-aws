# Layer — `itl-0004-itx-dev-lambda-layer-pyarrow-curl_cffi`

Layer con `pyarrow`+`curl_cffi` (HTTP con impersonación de TLS/browser
fingerprint), usado por `lmbd-mc-exchange-rates` para scrapear la API
pública del conversor de Mastercard vía proxies (`ProxyManager`, ver
`decisions.md` → "Por qué lmbd-mc-exchange-rates se reescribió con
scraping vía proxies").

**Nota de nomenclatura:** el nombre real del recurso en AWS
(`itl-0004-itx-dev-lambda-layer-pyarrow-curl_cffi`) no sigue la
convención del resto del proyecto (`itl-0004-itx-{env}-intchg-02-*`) —
no se tocó al documentarlo, solo se sincronizó el `.zip` fuente al repo.

`mastercard_layer.zip` es el paquete real subido a AWS (mismo nombre de
archivo que tiene en `s3-reference/layers/pyarrow-curl_cffi/`).
