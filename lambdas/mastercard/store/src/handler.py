"""
handler.py — Lambda real: itl-0004-itx-{env}-intchg-02-lmbd-mc-store
================================================================================
Archivo:     lambdas/mastercard/store/src/handler.py

Última etapa del pipeline Mastercard antes del archive. Consolida, por
cada MTI presente en el archivo, los Parquets de staging CLN + CAL + ITX
(este último solo para 1240/1442, donde existe capa de interchange) en un
único Parquet final y lo escribe en operational. A diferencia de Visa
(join por índice `record`), el merge es por llave de negocio explícita
`(file_id, file_idn, ref_id)` — ver `decisions.md` → "Por qué mc-store
fusiona CLN + CAL + ITX por llave (file_id, file_idn, ref_id) y no por
posición (axis=1)" — porque el orden posicional entre etapas Spark no está
garantizado entre ejecuciones. CLN se procesa en streaming
(`iter_batches`) para evitar OOM en archivos grandes (ver `decisions.md`
→ "Por qué lmbd-mc-store restaura el schema Arrow del CLN..."); CAL e ITX
se cargan completos en memoria (pequeños). `_restore_schema()` corrige
tipos degradados por el round-trip pandas/pyarrow tanto en columnas
heredadas de CLN como en columnas nuevas de CAL/ITX (NullType→string,
decimal de precisión no-18→decimal128(18,s), timestamps→microsegundos) —
necesario para que `spark.read.parquet()` pueda leer el directorio
operational sin `SchemaColumnConvertNotSupportedException`.

Mapeo de subdirectorios por MTI:
  400_IPM_{mti}_CLN  →  clean transactions  (entrada, recibida en store_input.outputs)
  500_IPM_{mti}_CAL  →  calculated fields   (Glue calculate)
  600_IPM_{mti}_ITX  →  interchange data    (Glue interchange — puede no existir)
  → operational: {client_id}/{brand_id}/IPM_{mti}/file_type={file_type}/date={date}/

MTIs soportados:
- 1240  (CLN + CAL + ITX si existe)
- 1442  (CLN + CAL + ITX si existe)
- 1644  (CLN + CAL,  ITX normalmente ausente → itx_s3_key = null)
- 1740  (CLN + CAL,  ITX normalmente ausente → itx_s3_key = null)

Variables de entorno:
  S3_BUCKET_STAGING      : bucket de origen (default: itl-0004-itx-dev-intchg-02-s3-staging)
  S3_BUCKET_OPERATIONAL  : bucket de destino (default: itl-0004-itx-dev-intchg-02-s3-operational)
  ITX_STORE_BATCH_SIZE   : filas por batch al leer CLN (default: 100000)

Input (Step Functions — Payload.$: "$"): el estado PrepareStoreInput
coloca los datos bajo $.store_input. Los campos de identidad (client_id,
file_id, …) están tanto en la raíz del estado SF como dentro de
store_input (fallback via `_field()` en el handler).
```
{
    "client_id":    "EBGR",
    "file_id":      "38B4968A...",
    "brand":        "MASTERCARD",
    "brand_id":     "MC",
    "file_type":    "IN",
    "file_date":    "2026-01-30",
    "content_hash": "...",
    "filename":     "T112T0....",
    "store_input": {
        "staging_bucket":     "itl-0004-itx-dev-intchg-02-s3-staging",
        "operational_bucket": "itl-0004-itx-dev-intchg-02-s3-operational",
        "outputs": [
            {"mti": "1240", "s3_key": "EBGR/MC/400_IPM_1240_CLN/...parquet"},
            {"mti": "1644", "s3_key": "EBGR/MC/400_IPM_1644_CLN/..._685.parquet"},
            {"mti": "1644", "s3_key": "EBGR/MC/400_IPM_1644_CLN/..._688.parquet"},
            {"mti": "1740", "s3_key": "EBGR/MC/400_IPM_1740_CLN/...parquet"},
        ],
        "client_id":    "EBGR",
        "file_id":      "38B4968A...",
        ...
    }
}
```

Output (alineado con el store de Visa):
```
{
    "status":        "SUCCESS" | "PARTIAL_SUCCESS" | "ERROR",
    "total_outputs": <int>,
    "total_records": <int>,
    "outputs": [
        {
            "mti":           "1240",
            "cln_s3_key":    "EBGR/MC/400_IPM_1240_CLN/…parquet",
            "cal_s3_key":    "EBGR/MC/500_IPM_1240_CAL/…parquet",
            "itx_s3_key":    "EBGR/MC/600_IPM_1240_ITX/…parquet",  ← null si no existe
            "target_s3_key": "EBGR/MC/IPM_1240/…parquet",
            "records":       <int>,
            "columns":       <int>,
            "batches":       <int>,
        },
        ...
    ],
    "errors":        null | [{"mti": "...", "s3_key": "...", "error": "..."}],
    "client_id":     "EBGR",
    "file_id":       "38B4968A...",
    "brand":         "MASTERCARD",
    "brand_id":      "MC",
    "file_type":     "IN",
    "file_date":     "2026-01-30",
    "content_hash":  "...",
    "filename":      "T112T0...",
}
```
"""

from __future__ import annotations

import gc
import io
import json
import logging
import os
from time import perf_counter
from typing import Any, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger()
log.setLevel(logging.INFO)

# ==============================================================================
# AWS clients — reutilizados entre warm starts
# ==============================================================================

S3 = boto3.client("s3")

S3_BUCKET_STAGING: str = os.environ.get(
    "S3_BUCKET_STAGING",
    "itl-0004-itx-dev-intchg-02-s3-staging",
)
S3_BUCKET_OPERATIONAL: str = os.environ.get(
    "S3_BUCKET_OPERATIONAL",
    "itl-0004-itx-dev-intchg-02-s3-operational",
)

# Filas por batch al iterar CLN (variable de entorno para ajuste fino)
STORE_BATCH_SIZE: int = int(os.environ.get("ITX_STORE_BATCH_SIZE", "100000"))

# MTIs soportados
SUPPORTED_MTIS: frozenset[str] = frozenset({"1240", "1442", "1644", "1740"})

# MTIs que tienen capa ITX (interchange)
MTIS_WITH_ITX: frozenset[str] = frozenset({"1240", "1442"})

# ==============================================================================
# S3 helpers
# ==============================================================================


def _download_to_bytesio(bucket: str, key: str) -> io.BytesIO:
    """
    Descarga un objeto S3 completo a memoria.

    Args:
        bucket: Bucket S3 donde está el objeto.
        key: Key del objeto.

    Returns:
        Buffer `BytesIO` posicionado al inicio, con el contenido completo.

    Ejemplo:
        _download_to_bytesio(bucket, "EBGR/MC/400_IPM_1240_CLN/.../x.parquet")
    """
    body = S3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return io.BytesIO(body)


def _read_parquet_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Lee un único Parquet desde S3 y lo devuelve como DataFrame de pandas.

    Args:
        bucket: Bucket S3 donde está el Parquet.
        key: Key del archivo Parquet.

    Returns:
        DataFrame con el contenido del Parquet.

    Ejemplo:
        _read_parquet_s3(bucket, "EBGR/MC/500_IPM_1240_CAL/.../x.parquet")
    """
    return pd.read_parquet(_download_to_bytesio(bucket, key))


def _read_parquet_arrow_s3(bucket: str, key: str) -> pa.Table:
    """
    Lee un único Parquet desde S3 como `pa.Table` — preserva el schema
    real de la columna (tipos exactos) antes de que un eventual round-trip
    por pandas lo degrade (INT64+nulls→float64, NullType, etc.).

    Args:
        bucket: Bucket S3 donde está el Parquet.
        key: Key del archivo Parquet.

    Returns:
        `pa.Table` con el contenido y schema originales del Parquet.

    Ejemplo:
        _read_parquet_arrow_s3(bucket, "EBGR/MC/500_IPM_1240_CAL/.../x.parquet")
    """
    return pq.read_table(_download_to_bytesio(bucket, key))


def _restore_schema(
    table: pa.Table,
    dtype_map: dict[str, pa.DataType],
    output_schema: Optional[pa.Schema] = None,
) -> pa.Table:
    """
    Restaura tipos Arrow degradados por el round-trip pandas/pyarrow, para
    que el Parquet final tenga el mismo schema físico entre archivos
    (necesario para que `spark.read.parquet()` lea el directorio completo
    sin `SchemaColumnConvertNotSupportedException`).

    - Columnas en dtype_map (schema autoritativo de CLN+CAL+ITX, capturado
      antes de convertir a pandas): castea al tipo original, excepto
      timestamps que siempre se fuerzan a "us".
    - Columnas sin dtype conocido (no en dtype_map): NullType→string,
      decimal128(p≠18,s)→decimal128(18,s), timestamp→"us".
    - Si se pasa output_schema (de un batch anterior): fuerza ese schema
      exacto para garantizar consistencia entre batches al escribir al
      ParquetWriter, en vez de derivar de dtype_map.

    Args:
        table: Tabla Arrow del batch actual (ya mergeada CLN+CAL+ITX).
        dtype_map: Schema autoritativo por nombre de columna, combinando
            los tipos originales de CAL, ITX y CLN (`cln_dtype_map` tiene
            prioridad en caso de colisión — ver `_store_output`). Ignorado
            si se pasa `output_schema`.
        output_schema: Si no es `None` (batches 2+), fuerza este schema
            exacto en vez de derivar uno nuevo desde `dtype_map`.

    Returns:
        La misma `pa.Table`, con las columnas casteadas a su tipo
        correcto. Si un cast individual falla, esa columna queda con su
        tipo inferido (se loguea un warning, no aborta el batch).

    Ejemplo:
        _restore_schema(batch_table, dtype_map)  # primer batch, deriva schema
        _restore_schema(batch_table, dtype_map, output_schema)  # batches siguientes
    """
    if output_schema is not None:
        # Modo batch 2+: castear al schema fijo del primer batch
        for field in output_schema:
            if field.name not in table.schema.names:
                continue
            current_type = table.schema.field(field.name).type
            if current_type == field.type:
                continue
            try:
                col_idx = table.schema.get_field_index(field.name)
                table = table.set_column(
                    col_idx,
                    field,
                    table.column(col_idx).cast(field.type, safe=False),
                )
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                log.warning(
                    "_restore_schema batch: no se pudo castear %s (%s→%s): %s",
                    field.name, current_type, field.type, exc,
                )
        return table

    # Modo primer batch: derivar schema desde dtype_map
    for i, field in enumerate(table.schema):
        current_type = field.type
        target_type: Optional[pa.DataType] = None

        if field.name in dtype_map:
            target_type = dtype_map[field.name]
            if pa.types.is_timestamp(target_type):
                target_type = pa.timestamp("us")
        elif pa.types.is_null(current_type):
            target_type = pa.string()
        elif pa.types.is_decimal(current_type) and current_type.precision != 18:
            target_type = pa.decimal128(18, current_type.scale)
        elif pa.types.is_timestamp(current_type):
            target_type = pa.timestamp("us")

        if target_type is not None and target_type != current_type:
            try:
                table = table.set_column(
                    i,
                    field.with_type(target_type),
                    table.column(i).cast(target_type, safe=False),
                )
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                log.warning(
                    "_restore_schema: no se pudo castear columna %s (%s -> %s): %s",
                    field.name, current_type, target_type, exc,
                )

    return table


def _derive_cal_key(cln_s3_key: str, mti: str) -> str:
    return cln_s3_key.replace(f"400_IPM_{mti}_CLN", f"500_IPM_{mti}_CAL", 1)


def _derive_itx_key(cln_s3_key: str, mti: str) -> str:
    return cln_s3_key.replace(f"400_IPM_{mti}_CLN", f"600_IPM_{mti}_ITX", 1)


def _derive_target_key(cln_s3_key: str, mti: str) -> str:
    return cln_s3_key.replace(f"400_IPM_{mti}_CLN", f"IPM_{mti}", 1)


def _normalize_merge_keys(df: pd.DataFrame, keys: list[str]) -> None:
    """
    Castea las columnas llave a `string` nullable y les aplica `.str.strip()`
    in-place, para que el merge por llave entre CLN/CAL/ITX no falle por
    diferencias de tipo (ej. int vs string) o espacios sobrantes.

    Args:
        df: DataFrame a normalizar (modificado in-place).
        keys: Nombres de columna llave a normalizar, ej.
            `["file_id", "file_idn", "ref_id"]`. Las que no estén
            presentes en `df` se ignoran.

    Returns:
        None — modifica `df` in-place.

    Ejemplo:
        _normalize_merge_keys(df_cal, ["file_id", "file_idn", "ref_id"])
    """
    for k in keys:
        if k in df.columns:
            df[k] = df[k].astype("string").str.strip()

# ==============================================================================
# Store por output entry — streaming sobre CLN
# ==============================================================================


def _store_output(
    output: dict,
    staging_bucket: str,
    operational_bucket: str,
) -> dict:
    """
    Consolida CLN + CAL + ITX de un MTI en un único Parquet, mergeando por
    llave `(file_id, file_idn, ref_id)`.

    CLN se abre como `ParquetFile` y se itera en batches de
    `STORE_BATCH_SIZE` filas (streaming, para evitar OOM con archivos
    grandes). CAL e ITX se cargan completos en memoria (son pequeños) y se
    reducen a solo `KEYS` + columnas nuevas (las que no están ya en CLN,
    para no duplicar) antes del merge — con `validate="one_to_one"` y un
    chequeo explícito de duplicados por `KEYS`, que aborta con
    `ValueError` si CAL/ITX no son únicos por llave (un `how="left"`
    silencioso con llaves duplicadas produciría fan-out difícil de
    detectar en Athena). CAL/ITX faltantes o con error de lectura se
    loguean como warning y el merge continúa solo con lo disponible (ITX
    en particular es normal que falte para MTIs 1644/1740). El schema de
    salida se deriva del primer batch (`_restore_schema` sin
    `output_schema`) y se fuerza en los siguientes, para que el
    `ParquetWriter` no falle por inconsistencia de schema entre batches.

    Args:
        output: Dict de un output de store_input, con `mti` y `s3_key`
            (key del Parquet CLN).
        staging_bucket: Bucket S3 de staging (origen de CLN/CAL/ITX).
        operational_bucket: Bucket S3 operational (destino del Parquet
            final).

    Returns:
        Dict con el resultado del store (`mti`, `cln_s3_key`, `cal_s3_key`,
        `itx_s3_key` —`None` si no existía—, `target_s3_key`, `records`,
        `columns`, `batches`).

    Raises:
        ValueError: si CLN/CAL/ITX no tienen las columnas llave, o si
            CAL/ITX tienen filas duplicadas por `KEYS`.

    Ejemplo:
        _store_output({'mti': '1240', 's3_key': 'EBGR/MC/400_IPM_1240_CLN/.../x.parquet'},
                       staging_bucket, operational_bucket)
    """
    KEYS = ["file_id", "file_idn", "ref_id"]

    mti = output["mti"]
    cln_s3_key = output["s3_key"]
    cal_s3_key = _derive_cal_key(cln_s3_key, mti)
    target_s3_key = _derive_target_key(cln_s3_key, mti)
    itx_s3_key_candidate = (
        _derive_itx_key(cln_s3_key, mti) if mti in MTIS_WITH_ITX else None
    )

    log.info("_store_output: START mti=%s | cln=%s", mti, cln_s3_key)
    t0 = perf_counter()

    # ── 1. Abrir CLN como ParquetFile (descarga completa, itera en batches) ──
    cln_buf = _download_to_bytesio(staging_bucket, cln_s3_key)
    cln_pf = pq.ParquetFile(cln_buf)
    cln_schema = cln_pf.schema_arrow
    cln_dtype_map = {f.name: f.type for f in cln_schema if f.name not in KEYS}
    cln_col_names = set(cln_schema.names)
    log.info(
        "_store_output: CLN opened | mti=%s | rows=%d cols=%d [%.2fs]",
        mti, cln_pf.metadata.num_rows, len(cln_schema), perf_counter() - t0,
    )

    # ── 2. Cargar CAL completo (pequeño) ──────────────────────────────────────
    df_cal: Optional[pd.DataFrame] = None
    new_cols_cal: list[str] = []
    cal_dtype_map: dict[str, pa.DataType] = {}
    try:
        # Arrow primero para capturar los tipos originales de cada columna --
        # el round-trip por pandas degrada INT64+nulls a float64 (numpy no
        # tiene int nullable). Se restaura más abajo junto con cln_dtype_map.
        cal_arrow = _read_parquet_arrow_s3(staging_bucket, cal_s3_key)
        df_cal_raw = cal_arrow.to_pandas()
        _normalize_merge_keys(df_cal_raw, KEYS)
        missing = [k for k in KEYS if k not in df_cal_raw.columns]
        if missing:
            raise ValueError(f"CAL no tiene llaves {missing}: {cal_s3_key}")
        dup_cal = df_cal_raw.duplicated(subset=KEYS).sum()
        if dup_cal > 0:
            raise ValueError(f"CAL tiene {dup_cal} duplicados por {KEYS}: {cal_s3_key}")
        new_cols_cal = [c for c in df_cal_raw.columns if c not in cln_col_names]
        cal_dtype_map = {f.name: f.type for f in cal_arrow.schema if f.name in new_cols_cal}
        df_cal = df_cal_raw[KEYS + new_cols_cal].copy() if new_cols_cal else None
        del df_cal_raw, cal_arrow
        gc.collect()
        log.info("_store_output: CAL loaded | mti=%s | new_cols=%d", mti, len(new_cols_cal))
    except S3.exceptions.NoSuchKey:
        log.warning("_store_output: CAL not found (skipping) | %s", cal_s3_key)
    except Exception as exc:
        log.warning("_store_output: CAL load error (skipping) | %s | %s", cal_s3_key, exc)
        df_cal = None
        new_cols_cal = []

    # ── 3. Cargar ITX completo (pequeño, si aplica) ───────────────────────────
    df_itx: Optional[pd.DataFrame] = None
    new_cols_itx: list[str] = []
    itx_dtype_map: dict[str, pa.DataType] = {}
    itx_s3_key_used: Optional[str] = None
    if itx_s3_key_candidate:
        try:
            itx_arrow = _read_parquet_arrow_s3(staging_bucket, itx_s3_key_candidate)
            df_itx_raw = itx_arrow.to_pandas()
            _normalize_merge_keys(df_itx_raw, KEYS)
            missing = [k for k in KEYS if k not in df_itx_raw.columns]
            if missing:
                raise ValueError(f"ITX no tiene llaves {missing}: {itx_s3_key_candidate}")
            dup_itx = df_itx_raw.duplicated(subset=KEYS).sum()
            if dup_itx > 0:
                raise ValueError(f"ITX tiene {dup_itx} duplicados por {KEYS}: {itx_s3_key_candidate}")
            already_cols = cln_col_names | set(new_cols_cal)
            new_cols_itx = [c for c in df_itx_raw.columns if c not in already_cols]
            itx_dtype_map = {f.name: f.type for f in itx_arrow.schema if f.name in new_cols_itx}
            df_itx = df_itx_raw[KEYS + new_cols_itx].copy() if new_cols_itx else None
            del df_itx_raw, itx_arrow
            gc.collect()
            itx_s3_key_used = itx_s3_key_candidate
            log.info("_store_output: ITX loaded | mti=%s | new_cols=%d", mti, len(new_cols_itx))
        except S3.exceptions.NoSuchKey:
            log.info("_store_output: ITX not found (itx_s3_key=null) | %s", itx_s3_key_candidate)
        except Exception as exc:
            log.warning("_store_output: ITX load error (skipping) | %s | %s", itx_s3_key_candidate, exc)
            df_itx = None
            new_cols_itx = []

    # cln_dtype_map queda al final para que gane en caso de colisión de
    # nombres (no debería ocurrir: new_cols_cal/new_cols_itx ya excluyen
    # columnas presentes en CLN).
    dtype_map = {**cal_dtype_map, **itx_dtype_map, **cln_dtype_map}

    # ── 4. Streaming: iterar CLN por batches, merge, escribir ────────────────
    output_buf = io.BytesIO()
    writer: Optional[pq.ParquetWriter] = None
    output_schema: Optional[pa.Schema] = None
    total_records = 0
    total_cols = 0
    batch_num = 0

    for arrow_batch in cln_pf.iter_batches(batch_size=STORE_BATCH_SIZE):
        batch_num += 1
        df_batch = arrow_batch.to_pandas()
        del arrow_batch
        _normalize_merge_keys(df_batch, KEYS)

        if batch_num == 1:
            missing = [k for k in KEYS if k not in df_batch.columns]
            if missing:
                raise ValueError(f"CLN no tiene llaves {missing}: {cln_s3_key}")

        # Merge con CAL
        if df_cal is not None:
            df_batch = df_batch.merge(df_cal, on=KEYS, how="left", validate="one_to_one")

        # Merge con ITX
        if df_itx is not None:
            df_batch = df_batch.merge(df_itx, on=KEYS, how="left", validate="one_to_one")

        batch_table = pa.Table.from_pandas(df_batch, preserve_index=False)
        del df_batch
        gc.collect()

        # Primer batch: derivar schema de salida; siguientes: forzarlo
        batch_table = _restore_schema(batch_table, dtype_map, output_schema)

        if writer is None:
            output_schema = batch_table.schema
            total_cols = batch_table.num_columns
            writer = pq.ParquetWriter(output_buf, output_schema, compression="snappy")

        batch_rows = batch_table.num_rows
        writer.write_table(batch_table)
        total_records += batch_rows

        del batch_table
        gc.collect()

        log.info(
            "_store_output: batch %d | mti=%s | rows=%d total=%d [%.2fs]",
            batch_num, mti, batch_rows, total_records, perf_counter() - t0,
        )

    # ── 5. Cerrar writer y subir a S3 ─────────────────────────────────────────
    if writer:
        writer.close()

    del cln_pf, cln_buf, df_cal, df_itx
    gc.collect()

    output_buf.seek(0)
    S3.put_object(Bucket=operational_bucket, Key=target_s3_key, Body=output_buf)
    del output_buf
    gc.collect()

    log.info(
        "_store_output: END mti=%s | records=%d cols=%d batches=%d [%.2fs]",
        mti, total_records, total_cols, batch_num, perf_counter() - t0,
    )

    return {
        "mti": mti,
        "cln_s3_key": cln_s3_key,
        "cal_s3_key": cal_s3_key,
        "itx_s3_key": itx_s3_key_used,
        "target_s3_key": target_s3_key,
        "records": total_records,
        "columns": total_cols,
        "batches": batch_num,
    }

# ==============================================================================
# Lambda handler
# ==============================================================================


def lambda_handler(event: dict, context: Any) -> dict:
    """
    Punto de entrada de la Lambda `lmbd-mc-store`. Invocada por la Step
    Function Mastercard tras `lmbd-mc-clean` (o manualmente para
    reprocesos, ver `.claude/memory/manual_execution.md` → "Reproceso
    manual lmbd-mc-store"). A diferencia de `lmbd-vi-store`, no actualiza
    DynamoDB `file_control` — eso lo hace la Step Function.

    Recibe el estado completo de Step Functions (`Payload.$: "$"`). El
    estado `PrepareStoreInput` coloca la información bajo `$.store_input`;
    los campos de identidad se leen de `store_input` con fallback a la
    raíz del evento (helper interno `_field()`). Recorre todos los
    outputs recibidos (uno por MTI, o dos para 1644 con Function Code
    685/688), descarta los MTI no soportados, hace el store de cada uno
    con `_store_output()` de forma independiente (un fallo en un MTI no
    aborta los demás) y agrega el resultado en un único payload de salida.

    Args:
        event: Payload de Step Functions — ver ejemplo completo en el
            docstring del módulo.
        context: Contexto de ejecución de Lambda; se usa
            `context.aws_request_id` para logging.

    Returns:
        Dict con `status` ("SUCCESS", "PARTIAL_SUCCESS" o "ERROR"),
        `total_outputs`, `total_records`, la lista `outputs` con el
        resultado de cada store exitoso, y `errors` con el detalle de los
        MTIs que fallaron — ver ejemplo completo en el docstring del
        módulo. Lanza `ValueError` si falta `client_id` o `file_id`.

    Ejemplo:
        lambda_handler({'client_id': 'EBGR', 'file_id': '38B4968A...',
                         'store_input': {'outputs': [...]}}, context)
    """
    log.info("REQUEST_ID=%s", context.aws_request_id)
    log.info("EVENT=%s", json.dumps(event))

    # ------------------------------------------------------------------
    # 1. Leer store_input (contiene outputs y buckets)
    # ------------------------------------------------------------------
    store_input: dict = event.get("store_input", {})

    # Campos de identidad — store_input tiene prioridad, luego root del evento
    def _field(name: str) -> Any:
        return store_input.get(name) or event.get(name)

    client_id    = _field("client_id")
    file_id      = _field("file_id")
    brand        = _field("brand")
    brand_id     = _field("brand_id")
    file_type    = _field("file_type")
    file_date    = _field("file_date")
    content_hash = _field("content_hash")
    filename     = _field("filename")

    # Buckets: store_input los tiene explícitos; fallback a env vars
    staging_bucket     = store_input.get("staging_bucket")     or S3_BUCKET_STAGING
    operational_bucket = store_input.get("operational_bucket") or S3_BUCKET_OPERATIONAL

    outputs: list[dict] = store_input.get("outputs", [])

    # ------------------------------------------------------------------
    # 2. Validación de campos obligatorios
    # ------------------------------------------------------------------
    if not client_id or not file_id:
        raise ValueError(
            f"Missing required fields: client_id={client_id!r}, file_id={file_id!r}"
        )

    if not outputs:
        log.warning("store_input.outputs is empty — nothing to store")
        return {
            "status":        "SUCCESS",
            "total_outputs": 0,
            "total_records": 0,
            "outputs":       [],
            "errors":        None,
            "client_id":     client_id,
            "file_id":       file_id,
            "brand":         brand,
            "brand_id":      brand_id,
            "file_type":     file_type,
            "file_date":     file_date,
            "content_hash":  content_hash,
            "filename":      filename,
        }

    log.info(
        "Processing store: client=%s brand=%s type=%s date=%s file_id=%s",
        client_id, brand, file_type, file_date, file_id,
    )
    log.info(
        "Outputs to store: %s",
        [(o.get("mti"), o.get("s3_key", "").rsplit("/", 1)[-1]) for o in outputs],
    )

    # ------------------------------------------------------------------
    # 3. Procesar cada output entry
    # ------------------------------------------------------------------
    t_global = perf_counter()
    store_outputs: list[dict] = []
    errors: list[dict] = []

    for output in outputs:
        mti = output.get("mti", "")

        if mti not in SUPPORTED_MTIS:
            log.warning("MTI %s no soportado — saltando: %s", mti, output.get("s3_key"))
            continue

        try:
            result = _store_output(
                output=output,
                staging_bucket=staging_bucket,
                operational_bucket=operational_bucket,
            )
            store_outputs.append(result)
        except Exception as exc:
            log.error(
                "FAILED store_output mti=%s s3_key=%s | error=%s",
                mti, output.get("s3_key"), exc,
                exc_info=True,
            )
            errors.append({
                "mti":    mti,
                "s3_key": output.get("s3_key"),
                "error":  str(exc),
            })

    # ------------------------------------------------------------------
    # 4. Métricas finales y respuesta
    # ------------------------------------------------------------------
    total_records = sum(o.get("records", 0) for o in store_outputs)

    status = (
        "ERROR"           if (errors and not store_outputs) else
        "PARTIAL_SUCCESS" if errors else
        "SUCCESS"
    )

    log.info(
        "=== Done: %d outputs, %d records stored | total_time=%.2fs ===",
        len(store_outputs), total_records, perf_counter() - t_global,
    )

    return {
        "status":        status,
        "total_outputs": len(store_outputs),
        "total_records": total_records,
        "outputs":       store_outputs,
        "errors":        errors if errors else None,
        "client_id":     client_id,
        "file_id":       file_id,
        "brand":         brand,
        "brand_id":      brand_id,
        "file_type":     file_type,
        "file_date":     file_date,
        "content_hash":  content_hash,
        "filename":      filename,
    }
