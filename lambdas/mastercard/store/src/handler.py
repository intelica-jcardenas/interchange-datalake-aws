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
            "batches":       1,
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
 
# MTIs soportados
SUPPORTED_MTIS: frozenset[str] = frozenset({"1240", "1442", "1644", "1740"})
 
# MTIs que tienen capa ITX (interchange)
MTIS_WITH_ITX: frozenset[str] = frozenset({"1240", "1442"})
 
# ==============================================================================
# S3 helpers
# ==============================================================================


def _read_parquet_s3(bucket: str, key: str) -> pd.DataFrame:
    """Lee un único archivo Parquet desde S3 y retorna un DataFrame."""
    body = S3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))
 
 
def _write_parquet_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Serializa un DataFrame como Parquet (snappy) y lo sube a S3."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    S3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    log.info("_write_parquet_s3: written → s3://%s/%s (%d rows)", bucket, key, len(df))
 
 
def _key_exists(bucket: str, key: str) -> bool:
    """Verifica si una clave existe en S3 sin descargar el objeto."""
    try:
        S3.head_object(Bucket=bucket, Key=key)
        return True
    except S3.exceptions.ClientError:
        return False
    except Exception:
        return False
 
 
def _derive_cal_key(cln_s3_key: str, mti: str) -> str:
    """Reemplaza el subdirectorio CLN por CAL en el s3_key."""
    return cln_s3_key.replace(f"400_IPM_{mti}_CLN", f"500_IPM_{mti}_CAL", 1)
 
 
def _derive_itx_key(cln_s3_key: str, mti: str) -> str:
    """Reemplaza el subdirectorio CLN por ITX en el s3_key."""
    return cln_s3_key.replace(f"400_IPM_{mti}_CLN", f"600_IPM_{mti}_ITX", 1)
 
 
def _derive_target_key(cln_s3_key: str, mti: str) -> str:
    """
    Construye la clave de destino en el bucket operational.
 
    Reemplaza el subdirectorio de staging:
        400_IPM_{mti}_CLN  →  IPM_{mti}
    """
    return cln_s3_key.replace(f"400_IPM_{mti}_CLN", f"IPM_{mti}", 1)

def _normalize_merge_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    df = df.copy()

    for k in keys:
        if k in df.columns:
            df[k] = df[k].astype("string").str.strip()

    return df

# ==============================================================================
# Store por output entry
# ==============================================================================
 
 
# def _store_output(
#     output: dict,
#     staging_bucket: str,
#     operational_bucket: str,
# ) -> dict:
#     """
#     Procesa una entrada de output (un archivo CLN) del store_input.outputs.
 
#     1. Lee el archivo CLN desde staging.
#     2. Intenta leer CAL y fusionarlo (columnas nuevas, mismo orden de filas).
#     3. Intenta leer ITX si el MTI lo soporta y el archivo existe.
#     4. Escribe el Parquet consolidado en el bucket operational.
#     5. Retorna el dict de resultado con cln/cal/itx/target s3 keys y métricas.
 
#     Parameters
#     ----------
#     output : dict
#         Entrada del array outputs: {"mti": "1240", "s3_key": "EBGR/MC/400_..."}
#     staging_bucket : str
#         Bucket S3 de staging (origen).
#     operational_bucket : str
#         Bucket S3 operational (destino).
 
#     Returns
#     -------
#     dict con: mti, cln_s3_key, cal_s3_key, itx_s3_key, target_s3_key,
#               records, columns, batches.
#     """
#     mti       = output["mti"]
#     cln_s3_key = output["s3_key"]
 
#     log.info("_store_output: START mti=%s | cln=%s", mti, cln_s3_key)
#     t0 = perf_counter()
 
#     cal_s3_key    = _derive_cal_key(cln_s3_key, mti)
#     target_s3_key = _derive_target_key(cln_s3_key, mti)
#     itx_s3_key_candidate = (
#         _derive_itx_key(cln_s3_key, mti) if mti in MTIS_WITH_ITX else None
#     )
 
#     # ── Leer CLN ──────────────────────────────────────────────────────────────
#     df_cln = _read_parquet_s3(staging_bucket, cln_s3_key)
#     log.info(
#         "_store_output: CLN read | mti=%s | rows=%d cols=%d [%.2fs]",
#         mti, len(df_cln), len(df_cln.columns), perf_counter() - t0,
#     )
#     frames = [df_cln]
 
#     # ── Leer CAL y fusionar (columnas nuevas únicamente) ──────────────────────
#     try:
#         df_cal = _read_parquet_s3(staging_bucket, cal_s3_key)
#         existing_cols = set(df_cln.columns)
#         new_cols = [c for c in df_cal.columns if c not in existing_cols]
#         if new_cols:
#             frames.append(df_cal[new_cols])
#         del df_cal
#         log.info("_store_output: CAL merged | mti=%s | new_cols=%d", mti, len(new_cols))
#     except S3.exceptions.NoSuchKey:
#         log.warning("_store_output: CAL not found (skipping) | %s", cal_s3_key)
#     except Exception as exc:
#         log.warning("_store_output: CAL read error (skipping) | %s | %s", cal_s3_key, exc)
 
#     # ── Leer ITX si aplica y existe ───────────────────────────────────────────
#     itx_s3_key_used: Optional[str] = None
 
#     if itx_s3_key_candidate:
#         try:
#             df_itx = _read_parquet_s3(staging_bucket, itx_s3_key_candidate)
#             already = set().union(*(set(f.columns) for f in frames))
#             itx_new_cols = [c for c in df_itx.columns if c not in already]
#             if itx_new_cols:
#                 frames.append(df_itx[itx_new_cols])
#             del df_itx
#             itx_s3_key_used = itx_s3_key_candidate
#             log.info(
#                 "_store_output: ITX merged | mti=%s | new_cols=%d",
#                 mti, len(itx_new_cols),
#             )
#         except S3.exceptions.NoSuchKey:
#             log.info(
#                 "_store_output: ITX not found (itx_s3_key=null) | %s",
#                 itx_s3_key_candidate,
#             )
#         except Exception as exc:
#             log.warning(
#                 "_store_output: ITX read error (skipping) | %s | %s",
#                 itx_s3_key_candidate, exc,
#             )


#     # ── Merge por índice posicional (misma cantidad de filas y mismo orden) ───
#     merged = pd.concat(frames, axis=1) if len(frames) > 1 else frames[0]
#     del frames
#     gc.collect()
 
#     records = len(merged)
#     columns = len(merged.columns)
 
#     # ── Escribir en operational ───────────────────────────────────────────────
#     _write_parquet_s3(merged, operational_bucket, target_s3_key)
#     del merged
#     gc.collect()
 
#     log.info(
#         "_store_output: END mti=%s | records=%d cols=%d [%.2fs]",
#         mti, records, columns, perf_counter() - t0,
#     )
 
#     return {
#         "mti":           mti,
#         "cln_s3_key":    cln_s3_key,
#         "cal_s3_key":    cal_s3_key,
#         "itx_s3_key":    itx_s3_key_used,
#         "target_s3_key": target_s3_key,
#         "records":       records,
#         "columns":       columns,
#         "batches":       1,
#     }


def _store_output(
    output: dict,
    staging_bucket: str,
    operational_bucket: str,
) -> dict:
    """
    Consolida CLN + CAL + ITX por llave:
      file_id, file_idn, ref_id

    Ya no concatena por posición.
    """
    KEYS = ["file_id", "file_idn", "ref_id"]

    mti = output["mti"]
    cln_s3_key = output["s3_key"]

    log.info("_store_output: START mti=%s | cln=%s", mti, cln_s3_key)
    t0 = perf_counter()

    cal_s3_key = _derive_cal_key(cln_s3_key, mti)
    target_s3_key = _derive_target_key(cln_s3_key, mti)
    itx_s3_key_candidate = (
        _derive_itx_key(cln_s3_key, mti) if mti in MTIS_WITH_ITX else None
    )

    # ── Leer CLN ─────────────────────────────────────────────
    df_cln = _read_parquet_s3(staging_bucket, cln_s3_key)
    df_cln = _normalize_merge_keys(df_cln, KEYS)

    missing_keys = [k for k in KEYS if k not in df_cln.columns]
    if missing_keys:
        raise ValueError(f"CLN no tiene llaves {missing_keys}: {cln_s3_key}")

    duplicated_cln = df_cln.duplicated(subset=KEYS).sum()
    if duplicated_cln > 0:
        raise ValueError(
            f"CLN tiene duplicados por {KEYS}: duplicated={duplicated_cln} | {cln_s3_key}"
        )

    merged = df_cln.copy()

    log.info(
        "_store_output: CLN read | mti=%s | rows=%d cols=%d [%.2fs]",
        mti,
        len(merged),
        len(merged.columns),
        perf_counter() - t0,
    )

    # ── Leer CAL y fusionar por llave ─────────────────────────
    try:
        df_cal = _read_parquet_s3(staging_bucket, cal_s3_key)
        df_cal = _normalize_merge_keys(df_cal, KEYS)
        
        missing_keys = [k for k in KEYS if k not in df_cal.columns]
        if missing_keys:
            raise ValueError(f"CAL no tiene llaves {missing_keys}: {cal_s3_key}")

        duplicated_cal = df_cal.duplicated(subset=KEYS).sum()
        if duplicated_cal > 0:
            raise ValueError(
                f"CAL tiene duplicados por {KEYS}: duplicated={duplicated_cal} | {cal_s3_key}"
            )

        existing_cols = set(merged.columns)
        new_cols = [c for c in df_cal.columns if c not in existing_cols]

        if new_cols:
            merged = merged.merge(
                df_cal[KEYS + new_cols],
                on=KEYS,
                how="left",
                validate="one_to_one",
            )

        log.info(
            "_store_output: CAL merged by keys | mti=%s | rows=%d new_cols=%d",
            mti,
            len(merged),
            len(new_cols),
        )

        del df_cal

    except S3.exceptions.NoSuchKey:
        log.warning("_store_output: CAL not found (skipping) | %s", cal_s3_key)

    except Exception as exc:
        log.warning(
            "_store_output: CAL read/merge error (skipping) | %s | %s",
            cal_s3_key,
            exc,
        )

    # ── Leer ITX si aplica y fusionar por llave ───────────────
    itx_s3_key_used = None

    if itx_s3_key_candidate:
        try:
            df_itx = _read_parquet_s3(staging_bucket, itx_s3_key_candidate)
            df_itx = _normalize_merge_keys(df_itx, KEYS)
            
            missing_keys = [k for k in KEYS if k not in df_itx.columns]
            if missing_keys:
                raise ValueError(
                    f"ITX no tiene llaves {missing_keys}: {itx_s3_key_candidate}"
                )

            duplicated_itx = df_itx.duplicated(subset=KEYS).sum()
            if duplicated_itx > 0:
                raise ValueError(
                    f"ITX tiene duplicados por {KEYS}: duplicated={duplicated_itx} | {itx_s3_key_candidate}"
                )

            existing_cols = set(merged.columns)
            itx_new_cols = [c for c in df_itx.columns if c not in existing_cols]

            if itx_new_cols:
                merged = merged.merge(
                    df_itx[KEYS + itx_new_cols],
                    on=KEYS,
                    how="left",
                    validate="one_to_one",
                )

            itx_s3_key_used = itx_s3_key_candidate

            log.info(
                "_store_output: ITX merged by keys | mti=%s | rows=%d new_cols=%d",
                mti,
                len(merged),
                len(itx_new_cols),
            )

            del df_itx

        except S3.exceptions.NoSuchKey:
            log.info(
                "_store_output: ITX not found (itx_s3_key=null) | %s",
                itx_s3_key_candidate,
            )

        except Exception as exc:
            log.warning(
                "_store_output: ITX read/merge error (skipping) | %s | %s",
                itx_s3_key_candidate,
                exc,
            )

    records = len(merged)
    columns = len(merged.columns)

    # ── Escribir operational ──────────────────────────────────
    _write_parquet_s3(merged, operational_bucket, target_s3_key)

    del merged
    gc.collect()

    log.info(
        "_store_output: END mti=%s | records=%d cols=%d [%.2fs]",
        mti,
        records,
        columns,
        perf_counter() - t0,
    )

    return {
        "mti": mti,
        "cln_s3_key": cln_s3_key,
        "cal_s3_key": cal_s3_key,
        "itx_s3_key": itx_s3_key_used,
        "target_s3_key": target_s3_key,
        "records": records,
        "columns": columns,
        "batches": 1,
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