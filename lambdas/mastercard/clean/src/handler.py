"""
Mastercard clean pipeline — AWS Lambda handler.
 
Reads extracted parquet files from S3 (EXT layer), casts and normalises every
column according to metadata-driven dtype definitions stored in DynamoDB,
enforces a deterministic column order, and writes the result back to S3 (CLN
layer) using a PyArrow schema.
 
Supported MTIs
--------------
- 1240
- 1442
- 1644  (filtered by Function Code: 685, 688, 691)
- 1740
 
Environment variables
---------------------
S3_BUCKET                  (required)  Main S3 bucket (staging).
S3_BUCKET_REFERENCE        (optional)  Reference data bucket.
                                        Default: "itl-0004-itx-dev-poc-02-reference"
DYNAMO_TABLE_FILE_CONTROL  (optional)  DynamoDB table with file metadata.
                                        Default: "itl-0004-itx-dev-dynamo-file_control-02"
 
S3 key structure
----------------
Input  (EXT): {client_id}/{brand_id}/{subdir}/file_type={file_type}/date={date}/
Output (CLN): {client_id}/{brand_id}/{subdir}/file_type={file_type}/date={date}/
 
Currency reference
------------------
s3://{S3_BUCKET_REFERENCE}/currency/data.parquet
Loaded once per process and cached at module level.
"""

from __future__ import annotations
 
import gc
import io
import json
import logging
import os
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from time import perf_counter
from typing import Any, Optional, Sequence
 
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
 
log = logging.getLogger()
log.setLevel(logging.INFO)
 
# ==============================================================================
# AWS clients — module-level for warm-start reuse
# ==============================================================================
 
S3 = boto3.client("s3")
DYNAMO = boto3.client("dynamodb")
 
S3_BUCKET: str = os.environ.get("S3_BUCKET", "itl-0004-itx-dev-intchg-02-s3-staging")
 
S3_BUCKET_REFERENCE: str = os.environ.get(
    "S3_BUCKET_REFERENCE",
    "itl-0004-itx-dev-intchg-02-s3-reference",
)
 
DYNAMO_TABLE_FILE_CONTROL: str = os.environ.get(
    "DYNAMO_TABLE_FILE_CONTROL",
    "itl-0004-itx-dev-dynamo-file_control-02",
)
 
DYNAMO_TABLE_FIELDS: str = "itl-0004-itx-dev-dynamo-mastercard_fields-02"
 
# ==============================================================================
# Business constants
# ==============================================================================
 
VALID_FC_1644: frozenset[str] = frozenset({"685", "688", "691"})
 
# Mandatory base columns prepended to every field-def table.
# MTIs 1240 and 1442 additionally carry file-level metadata columns.
_BASE_COLS: list[dict] = [
    {"extract_name": "file_idn", "data_type": "string"},
    {"extract_name": "file_dt", "data_type": "string"},
    {"extract_name": "type_mti", "data_type": "string"},
    {"extract_name": "ref_id", "data_type": "int64"},
    {"extract_name": "function_code", "data_type": "int64"},
]
 
_BASE_COLS_WITH_FILE_META: list[dict] = _BASE_COLS + [
    {"extract_name": "file_id", "data_type": "string"},
    {"extract_name": "file_processing_date", "data_type": "string"},
]
 
# Maps dynamic-decimal float_decimals flags to their currency-code column names.
_SCALE_TO_CURRENCY_COL: dict[int, str] = {
    -2: "currency_code_transaction_de_49",
    -3: "currency_code_reconciliation_de_50",
    -4: "currency_code_cardholder_billing_de_51",
}
 
# Compiled once at import time.
_WS_RE = re.compile(r"\s+")
 
# Used by _build_outputs_for_stepfunction to extract the MTI from an S3 key.
_MTI_FROM_KEY_RE = re.compile(r"/\d+_IPM_(\d{4})_\w+/")

# ==============================================================================
# Module-level caches
# ==============================================================================
 
# Raw DynamoDB scan rows — fetched once per process.
_fields_rows_cache: list[dict] = []
 
# Built field-def DataFrames keyed by variant tag ("default" / "with_file_cols").
_field_defs_cache: dict[str, pd.DataFrame] = {}
 
# Currency code → decimal places mapping — loaded from S3 once per process.
_currency_map_cache: Optional[dict[str, int | None]] = None
 
# ==============================================================================
# Decimal helpers
# ==============================================================================
 
 
def _quantize(d: Decimal, scale: int) -> Decimal:
    """Quantize a Decimal to ``scale`` fractional digits using ROUND_HALF_UP."""
    return d.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
 
 
def _to_implied_decimal(x: Any, scale: int) -> Optional[Decimal]:
    """
    Convert a digit-only string to Decimal by applying implied decimals.
 
    scale=2: "1234" → Decimal("12.34").  Values that already contain a decimal
    point are returned as-is to avoid double scaling.  Empty / NA → None.
    """
    if pd.isna(x) or x == "":
        return None
 
    s = str(x).strip()
    try:
        d = Decimal(s)
    except (InvalidOperation, ValueError):
        return None
 
    if "." not in s and scale > 0:
        d = d.scaleb(-scale)
    return d
 
 
def _to_scale_prefixed_decimal(
    x: Any, *, out_scale: Optional[int] = None
) -> Optional[Decimal]:
    """
    Parse a Mastercard scale-prefixed numeric value into a Decimal.
 
    Encoding: first digit = exponent, remaining digits = mantissa.
    Example: "212345" → exponent=2, mantissa=12345 → Decimal("123.45").
    Empty / NA → None.
    """
    if pd.isna(x) or x == "":
        return None
 
    s = re.sub(r"\.0$", "", str(x).strip()).replace(" ", "")
 
    if "." in s:
        try:
            d = Decimal(s)
        except (InvalidOperation, ValueError):
            return None
        return _quantize(d, out_scale) if out_scale is not None else d
 
    if not s.isdigit() or len(s) < 2:
        return None
 
    try:
        d = Decimal(s[1:]).scaleb(-int(s[0]))
    except (InvalidOperation, ValueError):
        return None
 
    return _quantize(d, out_scale) if out_scale is not None else d
 
 
def _to_dynamic_decimal(
    amount_str: Any,
    decimals: Any,
    *,
    default_decimals: int,
    out_scale: int,
) -> Optional[Decimal]:
    """
    Convert a digit-only amount string to Decimal using per-row currency decimals.
 
    Falls back to ``default_decimals`` when the row's decimals value is missing.
    Supports an optional leading '-' sign.  Empty / NA → None.
    """
    if pd.isna(amount_str):
        return None
 
    s = str(amount_str).strip()
    if not s:
        return None
 
    neg = s.startswith("-")
    digits = s[1:] if neg else s
 
    if not digits.isdigit():
        return None
 
    if decimals is None or pd.isna(decimals):
        decimals = default_decimals
 
    try:
        d = _quantize(Decimal(int(digits)).scaleb(-int(decimals)), out_scale)
        return -d if neg else d
    except (ValueError, InvalidOperation):
        return None


# ==============================================================================
# DynamoDB helpers
# ==============================================================================


def _dval(attr: dict) -> str:
    """Deserialise a DynamoDB attribute dict to a plain stripped string."""
    return str(attr.get("S") or attr.get("N") or "").strip()
 
 
def _get_fields_rows() -> list[dict]:
    """
    Return all rows from DYNAMO_TABLE_FIELDS, scanning DynamoDB only once.
 
    Uses Scan (dynamodb:Scan).  The table is small so a full scan is acceptable.
    Result is cached at module level for the process lifetime.
    """
    global _fields_rows_cache
    if _fields_rows_cache:
        return _fields_rows_cache
 
    rows: list[dict] = []
    kwargs: dict = {"TableName": DYNAMO_TABLE_FIELDS}
 
    while True:
        response = DYNAMO.scan(**kwargs)
        rows.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
 
    _fields_rows_cache = rows
    log.debug("_get_fields_rows: loaded %d rows (cached)", len(rows))
    return rows
 
 
def _load_field_defs(tag: str, *, with_file_cols: bool = False) -> pd.DataFrame:
    """
    Build and return the field-def DataFrame for the requested variant.
 
    Constructs the DataFrame from the cached DynamoDB scan, prepends the
    appropriate base columns, and caches the result by ``tag``.
 
    Parameters
    ----------
    tag:
        Cache key — use ``"default"`` for 1644/1740 and ``"with_file_cols"``
        for 1240/1442.
    with_file_cols:
        When True, appends file-level metadata columns (file_id, file_type,
        file_processing_date) to the base set.
    """
    if tag in _field_defs_cache:
        return _field_defs_cache[tag]
 
    records = [
        {
            "extract_name": _dval(item.get("column_name", {})),
            "data_type": _dval(item.get("data_type", {})),
            "float_decimals": _dval(item.get("float_decimals", {})) or None,
        }
        for item in _get_fields_rows()
    ]
 
    fd = pd.DataFrame(records)
    fd["extract_name"] = fd["extract_name"].astype(str).str.strip()
    fd["data_type"] = fd["data_type"].astype(str).str.strip().str.lower()
    fd["float_decimals"] = pd.to_numeric(fd["float_decimals"], errors="coerce").astype(
        "Int64"
    )
 
    # Prepend base columns; base definitions win on duplicates.
    base_cols = _BASE_COLS_WITH_FILE_META if with_file_cols else _BASE_COLS
    base_df = pd.DataFrame(base_cols)
    fd = (
        pd.concat([base_df, fd], ignore_index=True)
        .assign(extract_name=lambda d: d["extract_name"].astype(str).str.strip())
        .drop_duplicates(subset=["extract_name"], keep="first")
    )
 
    _field_defs_cache[tag] = fd
    log.debug("_load_field_defs: tag=%s built (%d rows, cached)", tag, len(fd))
    return fd


# ==============================================================================
# Currency reference
# ==============================================================================
 
 
def _get_currency_map() -> dict[str, int | None]:
    """
    Return the currency code → decimal places mapping.
 
    Loads ``s3://{S3_BUCKET_REFERENCE}/currency/data.parquet`` on the first
    call and caches the result for the process lifetime.
    """
    global _currency_map_cache
    if _currency_map_cache is not None:
        return _currency_map_cache
 
    body        = S3.get_object(Bucket=S3_BUCKET_REFERENCE, Key="currency/data.parquet")["Body"].read()
    currency_df = pd.read_parquet(io.BytesIO(body))
    result: dict[str, int | None] = {}
    for r in currency_df.to_dict(orient="records"):
        code = str(r["currency_numeric_code"]).zfill(3)
        dec = r.get("currency_decimal_separator")
        result[code] = None if (dec is None or str(dec).strip() == "") else int(dec)
 
    _currency_map_cache = result
    log.debug("_get_currency_map: loaded %d currencies (cached)", len(result))
    return result


# ==============================================================================
# DataFrame casting
# ==============================================================================


def _cast_df(
    df: pd.DataFrame,
    param: pd.DataFrame,
    *,
    date_format: str = "%Y%m%d",
    timestamp_format: Optional[str] = None,
    default_decimal_scale: int = 2,
    conversion_rate_scale: int = 9,
    dynamic_decimal_out_scale: int = 4,
    currency_decimals_map: Optional[dict[str, int | None]] = None,
) -> pd.DataFrame:
    """
    Cast DataFrame columns according to metadata-driven type definitions.
 
    Parameters
    ----------
    df:
        Input DataFrame from an extracted parquet file.
    param:
        Metadata table with columns ``extract_name``, ``data_type``, and
        optionally ``float_decimals``.
    date_format:
        strptime format for ``date`` columns.  Default ``"%Y%m%d"``.
    timestamp_format:
        strptime format for ``timestamp`` columns.  If None, pandas infers.
    default_decimal_scale:
        Implied-decimal fallback when ``float_decimals`` is missing / NA.
    conversion_rate_scale:
        Output scale for scale-prefixed decimals (``float_decimals == -1``).
    dynamic_decimal_out_scale:
        Output scale for dynamic decimals (``float_decimals`` in {-2, -3, -4}).
    currency_decimals_map:
        Pre-built currency → decimals mapping.  Required when the DataFrame
        contains dynamic-decimal columns.
 
    Returns
    -------
    pd.DataFrame
        New DataFrame with cast columns in metadata-driven column order
        (defined columns first, extra columns at the end).
 
    Notes
    -----
    Supported ``data_type`` values: ``int64``, ``string``, ``timestamp``,
    ``date``, ``time``, ``decimal``.
 
    ``float_decimals`` flags for decimal columns:
    - ``>= 0``    : implied decimals scale
    - ``-1``      : scale-prefixed (conversion rates)
    - ``-2/-3/-4``: dynamic implied decimals driven by DE_49/50/51 currency codes
    """
    out = df.copy()
 
    has_scale = "float_decimals" in param.columns
    cols = ["extract_name", "data_type"] + (["float_decimals"] if has_scale else [])
 
    p = param[cols].copy()
    p["extract_name"] = p["extract_name"].astype(str).str.strip()
    p["data_type"] = p["data_type"].astype(str).str.strip().str.lower()
    if has_scale:
        p["float_decimals"] = pd.to_numeric(
            p["float_decimals"], errors="coerce"
        ).astype("Int64")
 
    # Collect dynamic-decimal columns for a second pass after currency resolution.
    dynamic_fields: list[tuple[str, int]] = []
 
    for _, row in p.iterrows():
        col = row["extract_name"]
        t = row["data_type"]
 
        if col not in out.columns:
            continue
 
        if t == "int64":
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
 
        elif t == "string":
            out[col] = out[col].astype("string")
 
        elif t == "timestamp":
            if timestamp_format:
                s = (
                    out[col]
                    .astype("string")
                    .str.strip()
                    .str.replace(r"\.0$", "", regex=True)
                    .str.zfill(12)
                )
                out[col] = pd.to_datetime(s, format=timestamp_format, errors="coerce")
            else:
                out[col] = pd.to_datetime(out[col], errors="coerce")
 
        elif t == "date":
            s = (
                out[col]
                .astype("string")
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
                .str.zfill(6)
            )
            out[col] = pd.to_datetime(s, format=date_format, errors="coerce").dt.date
 
        elif t == "time":
            s = (
                out[col]
                .astype("string")
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
                .str.zfill(6)
            )
            out[col] = (
                s.str.slice(0, 2) + ":" + s.str.slice(2, 4) + ":" + s.str.slice(4, 6)
            ).astype("string")
 
        elif t == "decimal":
            scale = default_decimal_scale
            if has_scale and pd.notna(row["float_decimals"]):
                scale = int(row["float_decimals"])
 
            if scale == -1:
                src = out[col]
                converted = src.apply(
                    lambda v: _to_scale_prefixed_decimal(
                        v, out_scale=conversion_rate_scale
                    )
                )
                bad = int(src.notna().sum() - pd.Series(converted).notna().sum())
                if bad:
                    log.warning("cast column '%s': %d invalid values → NULL", col, bad)
                out[col] = converted
 
            elif scale in (-2, -3, -4):
                out[col] = out[col].astype("string")
                dynamic_fields.append((col, scale))
 
            else:
                src = out[col]
                converted = src.apply(lambda x: _to_implied_decimal(x, scale))
                bad = int(src.notna().sum() - pd.Series(converted).notna().sum())
                if bad:
                    log.warning("cast column '%s': %d invalid values → NULL", col, bad)
                out[col] = converted
 
        else:
            out[col] = out[col].astype("string")
 
    # Second pass: resolve dynamic decimal columns using per-row currency codes.
    if dynamic_fields:
        if currency_decimals_map is None:
            raise ValueError(
                "_cast_df: currency_decimals_map is required for dynamic decimal "
                "columns but was not provided."
            )
 
        for col, scale_flag in dynamic_fields:
            currency_col = _SCALE_TO_CURRENCY_COL.get(scale_flag)
            if not currency_col or currency_col not in out.columns:
                log.warning(
                    "cast column '%s': scale=%d but '%s' missing → NULL",
                    col,
                    scale_flag,
                    currency_col,
                )
                out[col] = None
                continue
 
            dec_series = (
                out[currency_col]
                .astype("string")
                .str.strip()
                .str.zfill(3)
                .map(currency_decimals_map)
            )
            converted: list[Optional[Decimal]] = [
                _to_dynamic_decimal(
                    amount_str=amt,
                    decimals=dec,
                    default_decimals=default_decimal_scale,
                    out_scale=dynamic_decimal_out_scale,
                )
                for amt, dec in zip(
                    out[col].astype("string").tolist(), dec_series.tolist()
                )
            ]
 
            bad = int(
                out[col].astype("string").notna().sum()
                - pd.Series(converted).notna().sum()
            )
            if bad:
                log.warning(
                    "cast column '%s': %d dynamic invalid values → NULL", col, bad
                )
            if dec_series.isna().any():
                log.warning(
                    "cast column '%s': some currencies not in reference table → "
                    "fallback decimals=%d applied",
                    col,
                    default_decimal_scale,
                )
 
            out[col] = converted
 
    # Enforce metadata column order: defined columns first, unknowns at the end.
    meta_cols = p["extract_name"].tolist()
    ordered = [c for c in meta_cols if c in out.columns]
    extras = [c for c in out.columns if c not in set(ordered)]
    return out[ordered + extras]


# ==============================================================================
# Arrow schema builder
# ==============================================================================


def _build_arrow_schema(
    param: pd.DataFrame,
    *,
    ordered_cols: Optional[Sequence[str]] = None,
    default_decimal_precision: int = 18,
    default_decimal_scale: int = 2,
    conversion_rate_scale: int = 9,
    timestamp_unit: str = "ns",
) -> pa.Schema:
    """
    Build a PyArrow schema that matches the metadata-driven casting rules.
 
    Parameters
    ----------
    param:
        Same metadata table passed to ``_cast_df``.
    ordered_cols:
        Final column order for the schema — should be ``list(df_cast.columns)``
        so the schema exactly matches the parquet being written.
        Columns present here but absent from metadata fall back to ``pa.string()``.
    default_decimal_precision:
        Precision for all ``pa.decimal128`` fields.
    default_decimal_scale:
        Scale fallback when ``float_decimals`` is missing / NA.
    conversion_rate_scale:
        Scale for scale-prefixed decimal fields (``float_decimals == -1``).
    timestamp_unit:
        Arrow timestamp unit (e.g. ``"ns"``).
    """
    has_scale = "float_decimals" in param.columns
    cols = ["extract_name", "data_type"] + (["float_decimals"] if has_scale else [])
 
    p = param[cols].copy()
    p["extract_name"] = p["extract_name"].astype(str).str.strip()
    p["data_type"] = p["data_type"].astype(str).str.strip().str.lower()
    if has_scale:
        p["float_decimals"] = pd.to_numeric(
            p["float_decimals"], errors="coerce"
        ).astype("Int64")
 
    type_map = dict(zip(p["extract_name"], p["data_type"]))
    scale_map = dict(zip(p["extract_name"], p["float_decimals"])) if has_scale else {}
 
    cols_out = (
        list(ordered_cols) if ordered_cols is not None else list(p["extract_name"])
    )
 
    fields: list[pa.Field] = []
    for col in cols_out:
        t = type_map.get(col, "string")
 
        if t == "int64":
            fields.append(pa.field(col, pa.int64()))
        elif t == "int32":
            fields.append(pa.field(col, pa.int32()))
        elif t == "timestamp":
            fields.append(pa.field(col, pa.timestamp(timestamp_unit)))
        elif t == "date":
            fields.append(pa.field(col, pa.date32()))
        elif t == "time":
            fields.append(pa.field(col, pa.string()))
        elif t == "decimal":
            scale = default_decimal_scale
            if has_scale and pd.notna(scale_map.get(col)):
                scale = int(scale_map[col])
            if scale == -1:
                fields.append(
                    pa.field(
                        col,
                        pa.decimal128(default_decimal_precision, conversion_rate_scale),
                    )
                )
            elif scale in (-2, -3, -4):
                fields.append(
                    pa.field(col, pa.decimal128(default_decimal_precision, 4))
                )
            else:
                fields.append(
                    pa.field(col, pa.decimal128(default_decimal_precision, scale))
                )
        else:
            fields.append(pa.field(col, pa.string()))
 
    return pa.schema(fields)


# ==============================================================================
# S3 helpers
# ==============================================================================
 
 
def _get_file_details(client_id: str, file_id: str) -> dict:
    """
    Retrieve file metadata from the DynamoDB file_control table via get_item.
 
    Returns a dict with keys: brand_id, file_type, file_processing_date,
    landing_file_name.  Raises ValueError if no record is found or the
    client_id does not match.
    """
    response = DYNAMO.get_item(
        TableName=DYNAMO_TABLE_FILE_CONTROL,
        Key={"file_id": {"S": file_id}},
    )
    item = response.get("Item")
 
    if not item:
        raise ValueError(
            f"file_control: no record found for "
            f"client_id={client_id!r}, file_id={file_id!r}"
        )
 
    if _dval(item.get("client_id", {})) != client_id:
        raise ValueError(
            f"file_control: file_id={file_id!r} does not belong to "
            f"client_id={client_id!r}"
        )
 
    return {
        "brand_id": _dval(item.get("brand_id", {})),
        "file_type": _dval(item.get("file_type", {})),
        "file_processing_date": _dval(item.get("file_processing_date", {})),
        "landing_file_name": _dval(item.get("landing_file_name", {})),
    }


def _s3_prefix(client_id: str, subdir: str, file_details: dict) -> str:
    """
    Build the S3 key prefix for a given client and subdirectory.
 
        {client_id}/{brand_id}/{subdir}/file_type={file_type}/date={date}/
    """
    parts = [
        client_id,
        file_details["brand_id"],
        subdir,
        f"file_type={file_details['file_type']}",
        f"date={file_details['file_processing_date']}",
    ]
    return "/".join(p for p in parts if p) + "/"
 
 
def _list_parquet_keys(prefix: str, file_id: str) -> list[str]:
    """
    List S3 keys of parquet files under prefix whose filename starts with file_id.
 
    Paginates automatically and returns results sorted by filename.
    """
    keys: list[str] = []
    paginator = S3.get_paginator("list_objects_v2")
 
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key.rsplit("/", 1)[-1]
            if name.startswith(file_id) and name.endswith(".parquet"):
                keys.append(key)
 
    keys.sort(key=lambda k: k.rsplit("/", 1)[-1])
    return keys
 
 
def _read_parquet(key: str) -> pd.DataFrame:
    """Download a parquet file from S3 and return it as a DataFrame."""
    body = S3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def _write_parquet_with_schema(df: pd.DataFrame, key: str, schema: pa.Schema) -> None:
    """
    Serialise a DataFrame as parquet using a PyArrow schema and upload it to S3.
 
    Only columns present in both df and schema are written, guaranteeing a
    schema-conformant output file.
    """
    schema_cols = [f.name for f in schema]
 
    df_aligned = df[[c for c in schema_cols if c in df.columns]].copy()
 
    # Align pandas dtypes with Arrow schema types before conversion.
    # This prevents ArrowTypeError caused by pandas dtype inference mismatches.
    for field in schema:
        col = field.name
 
        if col not in df_aligned.columns:
            continue
 
        if pa.types.is_string(field.type):
            df_aligned[col] = df_aligned[col].astype("string")
 
        elif pa.types.is_int64(field.type):
            df_aligned[col] = pd.to_numeric(
                df_aligned[col],
                errors="coerce",
            ).astype("Int64")
 
        elif pa.types.is_int32(field.type):
            df_aligned[col] = pd.to_numeric(
                df_aligned[col],
                errors="coerce",
            ).astype("Int32")

        elif pa.types.is_date(field.type):
            # Convert to Python datetime.date objects (or None for nulls).
            # Using native Python date objects is the only representation that
            # PyArrow reliably writes as date32 across all supported versions.
            # Relying on datetime64 → date32 auto-cast fails on older pyarrow
            # releases (e.g. Lambda environments) when the column is all-null,
            # producing timestamp[ms] or raising ArrowTypeError instead.
            dt_series = pd.to_datetime(df_aligned[col], errors="coerce")
            df_aligned[col] = [
                None if pd.isna(v) else v.date()
                for v in dt_series
            ]

    buf = io.BytesIO()
 
    pq.write_table(
        pa.Table.from_pandas(
            df_aligned,
            schema=schema,
            preserve_index=False,
        ),
        buf,
        compression="snappy",
    )
 
    S3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buf.getvalue(),
    )


def _target_key(
    raw_key: str, target_prefix: str, mti: str, fc: str | None = None
) -> str:
    """
    Derive the destination S3 key from the source key.
 
    Output filename:
    - MTI 1644 with FC  →  {md5}_{file_idn}_{mti}_{fc}.parquet
    - All others        →  {md5}_{file_idn}_{mti}.parquet
 
    Raises ValueError if the source filename does not match the expected pattern.
    """
    stem = Path(raw_key).stem
    has_raw = any("raw" in part.lower() for part in raw_key.split("/"))
 
    pattern = (
        r"^(?P<md5>[0-9a-fA-F]{32})_(?P<file_idn>[A-Za-z\d]{25})"
        r"_(?P<mti>\d{4})_(?P<fc>\d{3})$"
        if mti == "1644" and not has_raw and fc
        else r"^(?P<md5>[0-9a-fA-F]{32})_(?P<file_idn>[A-Za-z\d]{25})_(?P<mti>\d{4})$"
    )
 
    m = re.match(pattern, stem)
    if not m:
        raise ValueError(f"_target_key: unrecognised filename pattern: {stem!r}")
 
    mti_file = m.group("mti")
    filename = (
        f"{m.group('md5')}_{m.group('file_idn')}_{mti_file}_{fc}.parquet"
        if mti_file == "1644" and fc
        else f"{m.group('md5')}_{m.group('file_idn')}_{mti_file}.parquet"
    )
    return f"{target_prefix}{filename}"


# ==============================================================================
# MTI clean functions
# ==============================================================================


def _clean_1644(
    client_id: str,
    file_id: str,
    file_details: dict,
    origin_sub_dir: str = "300_IPM_1644_EXT",
    target_sub_dir: str = "400_IPM_1644_CLN",
    content_hash: str = "",
) -> None:
    """
    Clean MTI 1644 extracted parquet files.
 
    For each file: derive FC → skip unsupported FCs → read parquet → cast columns
    → build Arrow schema → write cleaned parquet to S3 → free memory.
 
    The Arrow schema is rebuilt per file because different FCs can produce
    different column sets.
    """
    origin_prefix = _s3_prefix(client_id, origin_sub_dir, file_details)
    target_prefix = _s3_prefix(client_id, target_sub_dir, file_details)
    list_keys = _list_parquet_keys(origin_prefix, file_id)
    log.info("MTI 1644 clean | %d files under %s", len(list_keys), origin_prefix)
 
    field_defs = _load_field_defs("default")
    currency_map = _get_currency_map()
 
    for key in list_keys:
        fc = Path(key).stem.rsplit("_", 1)[-1]
        if fc not in VALID_FC_1644:
            continue
 
        df = _read_parquet(key)
 
        df_cast = _cast_df(df=df, param=field_defs, currency_decimals_map=currency_map)
        del df

        # Schema rebuilt per file: different FCs yield different column sets.
        schema = _build_arrow_schema(
            field_defs,
            ordered_cols=list(df_cast.columns),
            default_decimal_precision=18,
            default_decimal_scale=2,
            timestamp_unit="ns",
        )
 
        out_key = _target_key(key, target_prefix, mti="1644", fc=fc)
        _write_parquet_with_schema(df_cast, out_key, schema)
        log.info("MTI 1644 clean | written → s3://%s/%s", S3_BUCKET, out_key)
 
        del df_cast
        gc.collect()


def _clean_standard(
    mti: str,
    client_id: str,
    file_id: str,
    file_details: dict,
    origin_sub_dir: str,
    target_sub_dir: str,
    *,
    date_format: str = "%y%m%d",
    timestamp_format: str = "%y%m%d%H%M%S",
    field_defs_tag: str,
    with_file_cols: bool = False,
    content_hash: str = "",
) -> None:
    """
    Shared clean pipeline for MTIs 1240, 1442, and 1740.
 
    For each file: read parquet → cast columns → write cleaned parquet to S3
    → free memory.
 
    The Arrow schema is built from the first file and reused for all subsequent
    files in the batch, since all files within the same MTI share the same
    column structure.
    """
    origin_prefix = _s3_prefix(client_id, origin_sub_dir, file_details)
    target_prefix = _s3_prefix(client_id, target_sub_dir, file_details)
    list_keys = _list_parquet_keys(origin_prefix, file_id)
    log.info("MTI %s clean | %d files under %s", mti, len(list_keys), origin_prefix)
 
    field_defs = _load_field_defs(field_defs_tag, with_file_cols=with_file_cols)
    currency_map = _get_currency_map()
 
    # Schema is built once from the first file and reused — all files in a
    # single MTI batch share the same column structure.
    schema: Optional[pa.Schema] = None
 
    for key in list_keys:
        df = _read_parquet(key)
 
        df_cast = _cast_df(
            df=df,
            param=field_defs,
            date_format=date_format,
            timestamp_format=timestamp_format,
            currency_decimals_map=currency_map,
        )
        del df

        if schema is None:
            schema = _build_arrow_schema(
                field_defs,
                ordered_cols=list(df_cast.columns),
                default_decimal_precision=18,
                default_decimal_scale=2,
                timestamp_unit="ns",
            )
 
        out_key = _target_key(key, target_prefix, mti=mti)
        _write_parquet_with_schema(df_cast, out_key, schema)
        log.info("MTI %s clean | written → s3://%s/%s", mti, S3_BUCKET, out_key)
 
        del df_cast
        gc.collect()


# Thin wrappers that bind each MTI to its subdirectories and field-def variant.
# To add a new MTI: write a wrapper here and add it to CLEANS.
 
 
def _clean_1240(client_id: str, file_id: str, file_details: dict, content_hash: str = "") -> None:
    """Clean MTI 1240: 300_IPM_1240_EXT → 400_IPM_1240_CLN."""
    _clean_standard(
        "1240",
        client_id,
        file_id,
        file_details,
        "300_IPM_1240_EXT",
        "400_IPM_1240_CLN",
        field_defs_tag="with_file_cols",
        with_file_cols=True,
        content_hash=content_hash,
    )
 
 
def _clean_1442(client_id: str, file_id: str, file_details: dict, content_hash: str = "") -> None:
    """Clean MTI 1442: 300_IPM_1442_EXT → 400_IPM_1442_CLN."""
    _clean_standard(
        "1442",
        client_id,
        file_id,
        file_details,
        "300_IPM_1442_EXT",
        "400_IPM_1442_CLN",
        field_defs_tag="with_file_cols",
        with_file_cols=True,
        content_hash=content_hash,
    )
 
 
def _clean_1740(client_id: str, file_id: str, file_details: dict, content_hash: str = "") -> None:
    """Clean MTI 1740: 300_IPM_1740_EXT → 400_IPM_1740_CLN."""
    _clean_standard(
        "1740",
        client_id,
        file_id,
        file_details,
        "300_IPM_1740_EXT",
        "400_IPM_1740_CLN",
        field_defs_tag="default",
        content_hash=content_hash,
    )


# ==============================================================================
# MTI dispatch map
# ==============================================================================
 
# Maps each MTI string to its clean function.
# To add a new MTI: write a wrapper above and add it here.
CLEANS: dict[str, Any] = {
    "1240": _clean_1240,
    "1442": _clean_1442,
    "1644": _clean_1644,
    "1740": _clean_1740,
}

# ==============================================================================
# Output builder — shared contract with mc_extract.py / mc_transform.py
# ==============================================================================
 
 
def _build_outputs_for_stepfunction(s3_urls: list[str]) -> list[dict]:
    """
    Convert the list of full S3 URLs written during cleaning into the
    structured array consumed by downstream Step Functions states.
 
    Input:  ["s3://bucket/SBSA/MC/400_IPM_1240_CLN/file_type=IN/date=.../xxx.parquet", ...]
    Output: [{"mti": "1240", "s3_key": "SBSA/MC/400_IPM_1240_CLN/file_type=IN/date=.../xxx.parquet"}, ...]
 
    Mirrors mc_extract._build_outputs_for_stepfunction exactly.
    """
    result: list[dict] = []
    for url in s3_urls:
        if url.startswith("s3://"):
            without_scheme = url[5:]                   # "bucket/rest/of/key"
            s3_key = without_scheme.split("/", 1)[1]   # "rest/of/key"
        else:
            s3_key = url
 
        m = _MTI_FROM_KEY_RE.search(url)
        mti = m.group(1) if m else "UNKNOWN"
 
        result.append({"mti": mti, "s3_key": s3_key})
    return result


# ==============================================================================
# Lambda handler
# ==============================================================================
 
 
def lambda_handler(event: dict, context: Any) -> dict:
    """
    AWS Lambda entry point for the Mastercard clean stage.
 
    Receives the full Step Functions state as the event payload
    (``Payload.$: "$"``).  Identity fields (client_id, file_id, …) are
    present at the event root level; the extract outputs that drive MTI
    detection live under ``$.clean_input.outputs`` as a list of
    ``{"mti": "...", "s3_key": "..."}`` objects — the same structure that
    mc_extract.py produces.
 
    Input event (flat, Step Functions contract)
    -------------------------------------------
    {
        "client_id":    "SBSA",
        "file_id":      "DD9D...",
        "brand":        "MASTERCARD",
        "brand_id":     "MC",
        "file_type":    "IN",
        "file_date":    "2026-02-18",
        "content_hash": "...",
        "filename":     "...",
        "clean_input": {
            "outputs": [
                {"mti": "1240", "s3_key": "SBSA/MC/300_IPM_1240_EXT/…parquet"},
                {"mti": "1644", "s3_key": "SBSA/MC/300_IPM_1644_EXT/…parquet"},
                ...
            ],
            ...
        },
        ...
    }
 
    Return (flat dict — aligned with mc_extract.py contract)
    ---------------------------------------------------------
    {
        "status":        "SUCCESS" | "ERROR",
        "total_outputs": <int>,
        "total_records": 0,
        "outputs": [
            {"mti": "1240", "s3_key": "SBSA/MC/400_IPM_1240_CLN/…parquet"},
            {"mti": "1644", "s3_key": "SBSA/MC/400_IPM_1644_CLN/…parquet"},
            ...
        ],
        "client_id":     "SBSA",
        "file_id":       "DD9D...",
        "brand":         "MASTERCARD",
        "brand_id":      "MC",
        "file_type":     "IN",
        "file_date":     "2026-02-18",
        "content_hash":  "...",
        "filename":      "...",
    }
    """
    log.info("REQUEST_ID=%s", context.aws_request_id)
    log.info("EVENT=%s", json.dumps(event))
 
    # ------------------------------------------------------------------
    # 1. Validate required environment variables
    # ------------------------------------------------------------------
    if not S3_BUCKET:
        raise ValueError("Missing required environment variable: S3_BUCKET")
 
    # ------------------------------------------------------------------
    # 2. Extract identity fields from event root
    #    Mirrors mc_extract.py field extraction pattern exactly.
    # ------------------------------------------------------------------
    client_id    = event.get("client_id")
    file_id      = event.get("file_id")
    brand        = event.get("brand")
    brand_id     = event.get("brand_id")
    file_type    = event.get("file_type")
    file_date    = event.get("file_date")
    content_hash = event.get("content_hash")
    filename     = event.get("filename")
 
    if not client_id or not file_id:
        raise ValueError(
            f"Missing required event fields: "
            f"client_id={client_id!r}, file_id={file_id!r}"
        )
 
    log.info(
        "Processing: client=%s, brand=%s, type=%s, date=%s, file_id=%s",
        client_id, brand, file_type, file_date, file_id,
    )
 
    # ------------------------------------------------------------------
    # 3. Derive MTIs from clean_input.outputs
    #    Reads the {"mti": "...", "s3_key": "..."} objects produced by
    #    mc_extract, mirroring how mc_extract derives MTIs from
    #    extract_input.outputs.
    # ------------------------------------------------------------------
    clean_input = event.get("clean_input", {})
    outputs = clean_input.get("outputs", [])
 
    mtis: list[str] = []
 
    if outputs:
        mtis_from_outputs = list({
            output["mti"]
            for output in outputs
            if output.get("mti") in CLEANS
        })
 
        if mtis_from_outputs:
            log.info("MTIs derived from clean_input.outputs: %s", mtis_from_outputs)
            mtis = mtis_from_outputs
        else:
            log.warning(
                "Could not derive MTIs from clean_input.outputs; "
                "falling back to all registered MTIs."
            )
            mtis = list(CLEANS.keys())
    else:
        log.info("clean_input.outputs is empty; using all registered MTIs.")
        mtis = list(CLEANS.keys())
 
    log.info("MTIs to process: %s", mtis)
 
    if not mtis:
        raise ValueError(
            f"No MTIs found to process: "
            f"client_id={client_id}, file_id={file_id}"
        )
 
    # ------------------------------------------------------------------
    # 4. Build file_details from event fields
    #    Avoids a redundant DynamoDB round-trip; all required fields are
    #    already present in the event (brand_id, file_type, file_date).
    # ------------------------------------------------------------------
    file_details: dict = {
        "brand_id":             brand_id or "",
        "file_type":            file_type or "",
        "file_processing_date": file_date or "",
        "landing_file_name":    filename or "",
    }
 
    # ------------------------------------------------------------------
    # 5. Run clean pipeline per MTI
    # ------------------------------------------------------------------
    t_global = perf_counter()
    mtis_ok: list[str] = []
 
    for mti in mtis:
        clean_fn = CLEANS.get(mti)
        if clean_fn is None:
            log.warning("MTI %s has no registered clean function; skipping", mti)
            continue
 
        log.info("START clean_%s", mti)
        t = perf_counter()
        clean_fn(client_id=client_id, file_id=file_id, file_details=file_details, content_hash=content_hash)
        log.info("END clean_%s | time=%.2fs", mti, perf_counter() - t)
        mtis_ok.append(mti)
 
    log.info(
        "=== Done: %d MTIs processed | total time=%.2fs ===",
        len(mtis_ok),
        perf_counter() - t_global,
    )
 
    # ------------------------------------------------------------------
    # 6. Collect real output paths written to 400_IPM_*_CLN
    #    Mirrors mc_extract's output collection from 300_IPM_*_EXT.
    # ------------------------------------------------------------------
    uploaded_outputs: list[str] = []
 
    for mti in mtis_ok:
        output_subdir = f"400_IPM_{mti}_CLN"
        prefix = _s3_prefix(client_id, output_subdir, file_details)
        keys = _list_parquet_keys(prefix, file_id)
        for key in keys:
            uploaded_outputs.append(f"s3://{S3_BUCKET}/{key}")
 
    log.info(
        "Outputs collected: %d parquets across %d MTIs",
        len(uploaded_outputs),
        len(mtis_ok),
    )
 
    uploaded_outputs_json = _build_outputs_for_stepfunction(uploaded_outputs)
 
    # ------------------------------------------------------------------
    # 7. Return flat response — aligned with mc_extract.py contract
    #    outputs is a list of {"mti": "...", "s3_key": "..."} objects,
    #    matching the structure produced by mc_extract and mc_transform.
    # ------------------------------------------------------------------
    return {
        "status":        "SUCCESS" if uploaded_outputs else "ERROR",
        "total_outputs": len(uploaded_outputs),
        "total_records": 0,
        "outputs":       uploaded_outputs_json,
        "client_id":     client_id,
        "file_id":       file_id,
        "brand":         brand,
        "brand_id":      brand_id,
        "file_type":     file_type,
        "file_date":     file_date,
        "content_hash":  content_hash,
        "filename":      filename,
    }