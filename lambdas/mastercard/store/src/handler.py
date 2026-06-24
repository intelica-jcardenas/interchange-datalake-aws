"""
Mastercard store pipeline — AWS Lambda handler.

Último paso del pipeline MC antes del archive.
Lee los Parquets ya procesados desde staging (CLN + CAL + ITX),
los consolida y los escribe en el bucket operational.

Mapeo de subdirectorios por MTI
--------------------------------
  400_IPM_{mti}_CLN  →  clean transactions  (entrada, recibida en store_input.outputs)
  500_IPM_{mti}_CAL  →  calculated fields   (Glue calculate)
  600_IPM_{mti}_ITX  →  interchange data    (Glue interchange — puede no existir)
  → operational: {client_id}/{brand_id}/IPM_{mti}/file_type={file_type}/date={date}/

MTIs soportados
---------------
- 1240  (CLN + CAL + ITX si existe)
- 1442  (CLN + CAL + ITX si existe)
- 1644  (CLN + CAL,  ITX normalmente ausente → itx_s3_key = null)
- 1740  (CLN + CAL,  ITX normalmente ausente → itx_s3_key = null)

Variables de entorno
--------------------
S3_BUCKET_STAGING      (opcional)  Bucket de origen.
                                    Default: "itl-0004-itx-dev-intchg-02-s3-staging"
S3_BUCKET_OPERATIONAL  (opcional)  Bucket de destino.
                                    Default: "itl-0004-itx-dev-intchg-02-s3-operational"
ITX_STORE_BATCH_SIZE   (opcional)  Filas por batch al leer CLN.  Default: 100000

Input (Step Functions — Payload.$: "$")
----------------------------------------
El estado PrepareStoreInput coloca los datos bajo $.store_input.
Los campos de identidad (client_id, file_id, …) están tanto en la raíz del
estado SF como dentro de store_input.

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

Output (alineado con vi_store.py)
-----------------------------------
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
    """Descarga un objeto S3 completo y retorna un BytesIO posicionado al inicio."""
    body = S3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return io.BytesIO(body)


def _read_parquet_s3(bucket: str, key: str) -> pd.DataFrame:
    """Lee un único Parquet desde S3 y retorna un DataFrame."""
    return pd.read_parquet(_download_to_bytesio(bucket, key))


def _restore_schema(
    table: pa.Table,
    cln_dtype_map: dict[str, pa.DataType],
    output_schema: Optional[pa.Schema] = None,
) -> pa.Table:
    """
    Restaura tipos Arrow degradados por el round-trip pandas/pyarrow.

    - Columnas en cln_dtype_map (schema autoritativo del CLN): castea al tipo
      original, excepto timestamps que siempre se fuerzan a "us".
    - Columnas CAL/ITX (no en cln_dtype_map): NullType→string,
      decimal128(p≠18,s)→decimal128(18,s), timestamp→"us".
    - Si se pasa output_schema (de un batch anterior): fuerza ese schema exacto
      para garantizar consistencia entre batches al escribir al ParquetWriter.
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

    # Modo primer batch: derivar schema desde cln_dtype_map
    for i, field in enumerate(table.schema):
        current_type = field.type
        target_type: Optional[pa.DataType] = None

        if field.name in cln_dtype_map:
            target_type = cln_dtype_map[field.name]
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
    Consolida CLN + CAL + ITX por llave (file_id, file_idn, ref_id).

    CLN se lee en streaming (iter_batches) para evitar OOM con archivos grandes.
    CAL e ITX se cargan completos en memoria (son pequeños).
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
    try:
        df_cal_raw = _read_parquet_s3(staging_bucket, cal_s3_key)
        _normalize_merge_keys(df_cal_raw, KEYS)
        missing = [k for k in KEYS if k not in df_cal_raw.columns]
        if missing:
            raise ValueError(f"CAL no tiene llaves {missing}: {cal_s3_key}")
        dup_cal = df_cal_raw.duplicated(subset=KEYS).sum()
        if dup_cal > 0:
            raise ValueError(f"CAL tiene {dup_cal} duplicados por {KEYS}: {cal_s3_key}")
        new_cols_cal = [c for c in df_cal_raw.columns if c not in cln_col_names]
        df_cal = df_cal_raw[KEYS + new_cols_cal].copy() if new_cols_cal else None
        del df_cal_raw
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
    itx_s3_key_used: Optional[str] = None
    if itx_s3_key_candidate:
        try:
            df_itx_raw = _read_parquet_s3(staging_bucket, itx_s3_key_candidate)
            _normalize_merge_keys(df_itx_raw, KEYS)
            missing = [k for k in KEYS if k not in df_itx_raw.columns]
            if missing:
                raise ValueError(f"ITX no tiene llaves {missing}: {itx_s3_key_candidate}")
            dup_itx = df_itx_raw.duplicated(subset=KEYS).sum()
            if dup_itx > 0:
                raise ValueError(f"ITX tiene {dup_itx} duplicados por {KEYS}: {itx_s3_key_candidate}")
            already_cols = cln_col_names | set(new_cols_cal)
            new_cols_itx = [c for c in df_itx_raw.columns if c not in already_cols]
            df_itx = df_itx_raw[KEYS + new_cols_itx].copy() if new_cols_itx else None
            del df_itx_raw
            gc.collect()
            itx_s3_key_used = itx_s3_key_candidate
            log.info("_store_output: ITX loaded | mti=%s | new_cols=%d", mti, len(new_cols_itx))
        except S3.exceptions.NoSuchKey:
            log.info("_store_output: ITX not found (itx_s3_key=null) | %s", itx_s3_key_candidate)
        except Exception as exc:
            log.warning("_store_output: ITX load error (skipping) | %s | %s", itx_s3_key_candidate, exc)
            df_itx = None
            new_cols_itx = []

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
        batch_table = _restore_schema(batch_table, cln_dtype_map, output_schema)

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
    AWS Lambda entry point para el paso Store del pipeline Mastercard.

    Recibe el estado completo de Step Functions (Payload.$: "$").
    El estado PrepareStoreInput coloca la información bajo $.store_input;
    los campos de identidad se leen de store_input con fallback al root del evento.

    Ver docstring del módulo para detalle de input/output.
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
