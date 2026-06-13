"""
Mastercard extraction pipeline — AWS Lambda handler.

Reads raw parquet files from S3, aligns their schema against the field layouts
stored in DynamoDB, renames technical column names to standardised extract names,
fills any missing layout columns with NA, reorders columns, and writes the result
back to S3.

Supported MTIs
--------------
- 1240
- 1442
- 1644  (filtered by Function Code: 685, 688, 691)
- 1740

Environment variables
---------------------
S3_BUCKET                  (required)  S3 bucket name.
DYNAMO_TABLE_FILE_CONTROL  (optional)  DynamoDB table with file metadata.
                                        Default: "itl-0004-itx-dev-dynamo-file_control-02"

S3 key structure
----------------
{client_id}/{brand_id}/{subdir}/file_type={file_type}/date={date}/
"""

from __future__ import annotations

import gc
import io
import json
import logging
import os
import re
from collections import defaultdict
from pathlib import Path
from time import perf_counter
from typing import Any, Iterable

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger()
log.setLevel(logging.INFO)

# ==============================================================================
# AWS clients — module-level so they are reused across Lambda warm starts
# ==============================================================================

S3 = boto3.client("s3")
DYNAMO = boto3.client("dynamodb")
 
S3_BUCKET = os.environ.get("S3_BUCKET")
 
DYNAMO_TABLE_FILE_CONTROL = os.environ.get("DYNAMO_TABLE_FILE_CONTROL")
 
# Stores DE/PDS field metadata and the extract column names.
DYNAMO_TABLE_FIELDS = os.environ.get("DYNAMO_TABLE_FIELDS")

# Rows read per batch when iterating parquet files with iter_batches.
# Avoids materialising the full decompressed DataFrame in RAM.
EXTRACT_BATCH_SIZE = int(os.environ.get("ITX_EXTRACT_BATCH_SIZE", "100000"))

# ==============================================================================
# Static business config — MTIs 1240 / 1442 / 1644 / 1740
# ==============================================================================

# Base columns that must appear first in every MTI 1644 extract output.
BASE_COLS_1644_EXTRACT = ["FILE_IDN", "FILE_DT", "MTI", "MSG_NO", "FUNCTION_CODE"]

# Semantic renames applied at the end of the MTI 1644 pipeline.
RENAME_COLS_1644 = {"MSG_NO": "ref_id", "MTI": "type_mti"}

# PDS tags to extract from DE_48 per Function Code in MTI 1644.
PDS_TAGS_BY_FC_1644: dict[str, set[int]] = {
    "685": {
        148, 
        165,
        300,
        302,
        358,
        370,
        372,
        374,
        378,
        380,
        381,
        384,
        390,
        391,
        392,
        393,
        394,
        395,
        396,
        400,
        401,
        402,
    },
    "688": {
        148,
        300,
        302,
        359,
        368,
        369,
        370,
        372,
        374,
        378,
        380,
        381,
        384,
        390,
        391,
        392,
        393,
        394,
        395,
        396,
        400,
        401,
        402,
    },
    "691": {5, 6, 25, 138, 165, 280},
}

# DE columns to include per Function Code in the MTI 1644 extract.
DE_COLS_BY_FC_1644: dict[str, list[str]] = {
    "685": ["DE_25", "DE_26", "DE_50", "DE_51"],
    "688": ["DE_25", "DE_26", "DE_50", "DE_51"],
    "691": [],
}

# PDS tags kept as scalar fields (not expanded into subfields) per FC.
PDS_FORCE_RAW_BY_FC_1644: dict[str, set[int]] = {
    "685": {400, 401, 402, 148, 300, 302, 374, 378},
    "688": {148, 368, 369, 300, 302, 374, 378, 400, 401, 402},
    "691": set(),
}

# Function Codes supported by the MTI 1644 pipeline.
VALID_FC_1644: frozenset[str] = frozenset({"685", "688", "691"})

# Fixed leading columns for the output of MTIs 1240, 1442, and 1740.
_FIRST_COLS = ["file_idn", "file_dt", "type_mti", "ref_id", "function_code"]

# Compiled once at import time — used by _missing_layout_keys on every file.
_RE_DE = re.compile(r"(?<![a-z0-9])de_\d+(?:_\d+)*", re.IGNORECASE)
_RE_PDS = re.compile(r"(?<![a-z0-9])pds_\d+(?:_\d+)*", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")

# Used by _build_outputs_for_stepfunction to extract the MTI from an S3 path.
_MTI_FROM_KEY_RE = re.compile(r"/\d+_IPM_(\d{4})_\w+/")

# ==============================================================================
# DynamoDB field metadata — single scan, shared cache
#
# _get_fields_rows() scans DYNAMO_TABLE_FIELDS once per process lifetime.
# _load_layout, _build_rename_map, and _fill_missing_cols all call it,
# so the table is only hit once regardless of how many MTIs are processed.
# ==============================================================================

_fields_rows_cache: list[dict] = []


def _get_fields_rows() -> list[dict]:
    """
    Return all rows from DYNAMO_TABLE_FIELDS, fetching from DynamoDB only once.

    Uses Scan (dynamodb:Scan) instead of PartiQL to avoid requiring the
    dynamodb:PartiQLSelect permission.  The table is small so a full scan is
    acceptable.  The result is cached at module level so every downstream
    function shares the same data.
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


def _dval(attr: dict) -> str:
    """
    Extract the value from a DynamoDB attribute dict.

    DynamoDB wraps every attribute in a type envelope, e.g. {"S": "value"} for
    strings or {"N": "42"} for numbers.  Returns a stripped plain string.
    """
    return str(attr.get("S") or attr.get("N") or "").strip()


# ==============================================================================
# Layout loader
# ==============================================================================

_layout_cache: dict[str, tuple[dict, dict]] = {}


def _load_layout(mti: str) -> tuple[dict, dict]:
    """
    Build and return (dict_de, dict_pds) layout dicts for the given MTI.

    Filters the cached field rows by type_mti and groups them into two dicts:
    - dict_de  : DE fields   e.g. {"DE_4": 14} or {"DE_3": {"DE_3_1": 2, ...}}
    - dict_pds : PDS fields  same structure

    Reconstruction rules
    --------------------
    - subfield == 0  →  scalar:    {"DE_4": 14}
    - subfield != 0  →  subfields: {"DE_3": {"DE_3_1": 2, "DE_3_2": 4}}
    - If a tag has both subfield=0 and subfield>0 rows, the subfield dict wins.
    """
    if mti in _layout_cache:
        return _layout_cache[mti]

    rows = [
        item for item in _get_fields_rows() if mti in _dval(item.get("type_mti", {}))
    ]

    if not rows:
        log.warning("_load_layout: no rows found for MTI=%s", mti)
        _layout_cache[mti] = ({}, {})
        return {}, {}

    groups: dict[tuple[str, int], list[tuple[int, int]]] = defaultdict(list)
    for item in rows:
        tlv = _dval(item.get("type_record", {})).upper()
        tag = int(float(_dval(item.get("tag", {"N": "0"}))))
        sub = int(float(_dval(item.get("subfield", {"N": "0"}))))
        leng = int(float(_dval(item.get("length", {"N": "0"}))))
        groups[(tlv, tag)].append((sub, leng))

    dict_de: dict = {}
    dict_pds: dict = {}

    for (tlv, tag), entries in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        field_key = f"{tlv}_{tag}"
        target = dict_de if tlv == "DE" else dict_pds
        sub_entries = [(s, l) for s, l in entries if s != 0]
        top_entries = [(s, l) for s, l in entries if s == 0]

        if sub_entries:
            target[field_key] = {f"{tlv}_{tag}_{s}": l for s, l in sorted(sub_entries)}
        elif top_entries:
            target[field_key] = top_entries[0][1]

    _layout_cache[mti] = (dict_de, dict_pds)
    log.debug(
        "_load_layout: MTI=%s → %d DE, %d PDS (cached)",
        mti,
        len(dict_de),
        len(dict_pds),
    )
    return dict_de, dict_pds


# ==============================================================================
# Rename map and missing-column filler
# ==============================================================================


def _build_rename_map() -> dict[str, str]:
    """
    Return a {field_mc: column_name} dict for DataFrame.rename().

    field_mc is built from column_name + tag + subfield:
    - subfield == "0"  →  "DE_4"    (no subfield suffix)
    - subfield != "0"  →  "DE_3_1"  (subfield appended)

    First occurrence of each field_mc wins; duplicates are ignored.
    """
    rename_map: dict[str, str] = {}

    for item in _get_fields_rows():
        tlv = _dval(item.get("type_record", {}))
        tag = _dval(item.get("tag", {}))
        sub = _dval(item.get("subfield", {}))
        col = _dval(item.get("column_name", {}))

        if not col:
            continue

        field_mc = f"{tlv}_{tag}" + (f"_{int(sub)}" if sub and sub != "0" else "")
        rename_map.setdefault(field_mc, col)

    return rename_map


def _fill_missing_cols(df: pd.DataFrame, missing_tokens: list[str]) -> pd.DataFrame:
    """
    Add missing layout columns to the DataFrame in-place, set to pd.NA.

    For each token (e.g. "de_25", "pds_358_1"), looks up the canonical
    column_name in the cached field rows and adds the column if absent.
    Operates directly on df to avoid creating an extra copy in memory.
    Tokens not found in the metadata are silently skipped.
    """
    for token in missing_tokens:
        parts = token.split("_")
        tlv = parts[0].upper()
        tag = parts[1]
        sub = parts[2] if len(parts) == 3 else "0"

        match = next(
            (
                item
                for item in _get_fields_rows()
                if (
                    _dval(item.get("column_name", {})).upper() == tlv
                    and _dval(item.get("tag", {})) == tag
                    and _dval(item.get("subfield", {})) == sub
                )
            ),
            None,
        )

        if match is None:
            continue

        col_name = _dval(match.get("column_name", {}))
        if col_name:
            extract_name = _normalize_col(col_name)
            if extract_name not in df.columns:
                df[extract_name] = pd.NA

    return df


# ==============================================================================
# Layout key helpers
# ==============================================================================


def _build_ordered_extract_cols(*layouts: dict[str, Any]) -> list[str]:
    """
    Merge layout dicts into an ordered, deduplicated list of extract column names.

    Traverses each dict depth-first to collect all keys (including nested subfield
    keys), then resolves each key to its extract column_name via the cached field
    rows.  Returns the names in layout order, deduplicated.
    """
    # Collect all layout keys depth-first, preserving order.
    layout_keys: list[str] = []

    def _walk(d: dict[str, Any]) -> None:
        for k, v in d.items():
            layout_keys.append(k)
            if isinstance(v, dict):
                _walk(v)

    for layout in layouts:
        _walk(layout)

    layout_keys = list(dict.fromkeys(layout_keys))

    # Parse each key into (tlv, tag, subfield) for the DynamoDB lookup.
    wanted: list[tuple[str, str, str]] = []
    for k in layout_keys:
        parts = k.split("_")
        if len(parts) < 2:
            continue
        wanted.append((parts[0].upper(), parts[1], parts[2] if len(parts) > 2 else "0"))

    # Build a (tlv, tag, subfield) → normalised column_name mapping.
    col_mapping: dict[tuple[str, str, str], str] = {}
    for item in _get_fields_rows():
        key = (
            _dval(item.get("column_name", {})).upper(),
            _dval(item.get("tag", {})),
            _dval(item.get("subfield", {})),
        )
        col = _dval(item.get("column_name", {}))
        if col:
            col_mapping.setdefault(key, _normalize_col(col))

    seen: set[str] = set()
    result: list[str] = []
    for key in wanted:
        name = col_mapping.get(key)
        if name and name not in seen:
            result.append(name)
            seen.add(name)

    return result


# ==============================================================================
# S3 helpers
# ==============================================================================


def _get_file_details(client_id: str, file_id: str) -> dict:
    """
    Retrieve file metadata from the DynamoDB file_control table.

    Uses get_item (dynamodb:GetItem) by file_id (partition key).
    Raises ValueError if no record is found or client_id does not match.

    Returns a dict with keys: brand_id, file_type, file_processing_date,
    landing_file_name.
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


def _write_parquet(df: pd.DataFrame, key: str) -> None:
    """Serialise a DataFrame as parquet (snappy) and upload it to S3."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy",coerce_timestamps="us")
    S3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())


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
# Column helpers
# ==============================================================================


def _normalize_col(name: object) -> str:
    """Strip, lowercase, and replace whitespace with underscores in a column name."""
    return _WS_RE.sub("_", str(name).strip().lower())


def _missing_layout_keys(df: pd.DataFrame, expected_keys: Iterable[str]) -> list[str]:
    """
    Return layout keys that are expected but absent from the DataFrame columns.

    Scans column names for DE_* and PDS_* tokens and returns any expected keys
    not found.
    """
    found: set[str] = set()
    for col in df.columns:
        s = str(col).lower()
        found.update(m.group(0) for m in _RE_DE.finditer(s))
        found.update(m.group(0) for m in _RE_PDS.finditer(s))

    return sorted(k for k in (str(k).lower() for k in expected_keys) if k not in found)


def _reorder_cols(
    df: pd.DataFrame,
    ordered_layout_cols: Iterable[str],
    first_cols: list[str],
) -> pd.DataFrame:
    """
    Reorder DataFrame columns: first_cols → layout columns → remaining extras.

    Column names are normalised in-place before reordering.  Columns not in
    either list are preserved at the end.  No full DataFrame copy is made —
    only a new column-order view is returned.
    """
    df.columns = [_normalize_col(c) for c in df.columns]
    cols = list(df.columns)
    first_n = [_normalize_col(c) for c in first_cols]
    layout_n = [_normalize_col(c) for c in ordered_layout_cols]

    first = [c for c in first_n if c in cols]
    used = set(first)
    layout = [c for c in layout_n if c in cols and c not in used]
    used.update(layout)
    extras = [c for c in cols if c not in used]

    # Column selection returns a view or lightweight copy — no full data copy.
    return df[first + layout + extras]


# ==============================================================================
# MTI 1644 — schema alignment
# ==============================================================================


def _align_df_1644(df: pd.DataFrame, fc: str, pds_layout: dict) -> pd.DataFrame:
    """
    Select and order MTI 1644 columns for the given Function Code.

    Keeps:
    - Base extract columns
    - Technical metadata columns
    - FC-specific DE columns
    - FC-specific PDS tags/subfields

    Missing columns are created as pd.NA.
    """
    if df is None or df.empty:
        return df

    de_cols = DE_COLS_BY_FC_1644.get(fc, [])
    tags = PDS_TAGS_BY_FC_1644.get(fc, set())
    force_raw = PDS_FORCE_RAW_BY_FC_1644.get(fc, set())

    # ------------------------------------------------------------------
    # Always-preserved metadata / technical columns
    # ------------------------------------------------------------------
    technical_cols = [
        "BLOCK",
        "ENC",
        "FUNCTION_ROLE",
        "PARSE_OK",
        "DE_1",
        "DE_48",
    ]

    # ------------------------------------------------------------------
    # Build FC-specific PDS list
    # ------------------------------------------------------------------
    fc_cols: list[str] = list(de_cols)

    for tag in sorted(tags):
        key = f"PDS_{tag}"
        spec = pds_layout.get(key)

        if spec is None:
            continue

        # Keep scalar parent field
        fc_cols.append(key)

        # Expand subfields unless forced raw
        if tag not in force_raw and isinstance(spec, dict):
            fc_cols.extend(spec.keys())

    # ------------------------------------------------------------------
    # Deduplicate while preserving order
    # ------------------------------------------------------------------
    seen: set[str] = set()

    fc_cols = [c for c in fc_cols if not (c in seen or seen.add(c))]

    # ------------------------------------------------------------------
    # Final wanted order
    # ------------------------------------------------------------------
    wanted = (
        [c for c in BASE_COLS_1644_EXTRACT if c in df.columns]
        + [c for c in technical_cols if c in df.columns]
        + fc_cols
    )

    # ------------------------------------------------------------------
    # Create missing columns
    # ------------------------------------------------------------------
    for col in wanted:
        if col not in df.columns:
            df[col] = pd.NA

    extras = [c for c in df.columns if c not in set(wanted)]
    return df[wanted + extras]


# ==============================================================================
# MTI extract functions
# ==============================================================================


def _extract_1644(
    client_id: str,
    file_id: str,
    file_details: dict,
    origin_sub_dir: str = "200_IPM_1644_TRA",
    target_sub_dir: str = "300_IPM_1644_EXT",
    content_hash: str = "",
) -> None:
    """
    Extract and standardise MTI 1644 parquet files.

    For each file: derive FC from filename → skip unsupported FCs → read parquet
    → inject FUNCTION_CODE → align schema by FC → rename columns → apply semantic
    renames → normalise column names → write to S3 → free memory.
    """
    origin_prefix = _s3_prefix(client_id, origin_sub_dir, file_details)
    target_prefix = _s3_prefix(client_id, target_sub_dir, file_details)
    list_keys = _list_parquet_keys(origin_prefix, file_id)
    log.info("MTI 1644 | %d files under %s", len(list_keys), origin_prefix)

    _, pds_layout = _load_layout("1644")
    print(_, "ASDSADSDJHFGBJSBGJHBH", pds_layout)
    rename_map = _build_rename_map()

    for key in list_keys:
        fc = Path(key).stem.rsplit("_", 1)[-1]
        if fc not in VALID_FC_1644:
            continue

        # ── Lectura en streaming: nunca se materializa el DataFrame completo ──
        # 1) Descargar bytes comprimidos y crear un buffer seekable.
        body = S3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        in_buf = io.BytesIO(body)
        del body   # libera la copia original comprimida
        gc.collect()

        pf = pq.ParquetFile(in_buf)    # abre el archivo (solo lee el footer)
        out_key = _target_key(key, target_prefix, mti="1644", fc=fc)
        out_buf = io.BytesIO()
        writer = None

        try:
            # 2) Iterar de a EXTRACT_BATCH_SIZE filas — nunca el DataFrame entero.
            for batch in pf.iter_batches(batch_size=EXTRACT_BATCH_SIZE):
                df = batch.to_pandas()

                df["FUNCTION_CODE"] = fc
                df = _align_df_1644(df, fc, pds_layout)
                df = df.rename(columns={**rename_map, **RENAME_COLS_1644})
                df.columns = [_normalize_col(c) for c in df.columns]

                # 3) Escribir el batch al ParquetWriter en memoria.
                table = pa.Table.from_pandas(df, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(out_buf, table.schema, compression="snappy")
                writer.write_table(table)

                del df, table
                gc.collect()
        finally:
            if writer is not None:
                writer.close()

        # 4) Subir el parquet acumulado en out_buf directamente a S3.
        del in_buf
        out_buf.seek(0)
        S3.put_object(Bucket=S3_BUCKET, Key=out_key, Body=out_buf)
        del out_buf
        gc.collect()

        log.info("MTI 1644 | written → s3://%s/%s", S3_BUCKET, out_key)


def _extract_standard(
    mti: str,
    client_id: str,
    file_id: str,
    file_details: dict,
    origin_sub_dir: str,
    target_sub_dir: str,
    content_hash: str = "",
) -> None:
    """
    Shared extract pipeline for MTIs 1240, 1442, and 1740.

    For each file: read parquet → rename columns → normalise names → fill any
    missing layout columns with NA → reorder columns → write to S3 → free memory.
    """
    origin_prefix = _s3_prefix(client_id, origin_sub_dir, file_details)
    target_prefix = _s3_prefix(client_id, target_sub_dir, file_details)
    list_keys = _list_parquet_keys(origin_prefix, file_id)
    log.info("MTI %s | %d files under %s", mti, len(list_keys), origin_prefix)

    dict_de, dict_pds = _load_layout(mti)
    rename_map = _build_rename_map()
    ordered_layout_cols = _build_ordered_extract_cols(dict_de, dict_pds)
    expected_keys = list(dict_de.keys()) + list(dict_pds.keys())

    for key in list_keys:
        # ── Lectura en streaming: nunca se materializa el DataFrame completo ──
        # 1) Descargar bytes comprimidos y crear un buffer seekable.
        body = S3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        in_buf = io.BytesIO(body)
        del body   # libera la copia original comprimida
        gc.collect()

        pf = pq.ParquetFile(in_buf)    # abre el archivo (solo lee el footer)
        out_key = _target_key(key, target_prefix, mti=mti)
        out_buf = io.BytesIO()
        writer = None

        # Estas listas se calculan una sola vez desde el primer batch:
        # los nombres de columna son iguales en todos los batches del mismo archivo.
        cols_to_drop: list[str] | None = None
        missing: list[str] | None = None

        try:
            # 2) Iterar de a EXTRACT_BATCH_SIZE filas — nunca el DataFrame entero.
            for batch in pf.iter_batches(batch_size=EXTRACT_BATCH_SIZE):
                df = batch.to_pandas()

                df = df.rename(columns=rename_map)
                df.columns = [_normalize_col(c) for c in df.columns]

                # 3) Calcular cols_to_drop y missing en el primer batch
                #    (los nombres de columna no cambian entre batches del mismo archivo).
                if cols_to_drop is None:
                    cols_to_drop = []
                    for layout_key, spec in {**dict_de, **dict_pds}.items():
                        if isinstance(spec, dict):
                            parent = _normalize_col(layout_key)
                            children = [_normalize_col(k) for k in spec.keys()]
                            if parent in df.columns and any(c in df.columns for c in children):
                                cols_to_drop.append(parent)

                    missing = _missing_layout_keys(df, expected_keys)
                    if missing:
                        log.warning(
                            "MTI %s | missing layout fields: %s%s",
                            mti,
                            missing[:20],
                            " ..." if len(missing) > 20 else "",
                        )

                if cols_to_drop:
                    df = df.drop(columns=cols_to_drop)
                if missing:
                    df = _fill_missing_cols(df, missing)

                df = _reorder_cols(df, ordered_layout_cols, _FIRST_COLS)

                # 4) Escribir el batch al ParquetWriter en memoria.
                table = pa.Table.from_pandas(df, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(out_buf, table.schema, compression="snappy")
                writer.write_table(table)

                del df, table
                gc.collect()
        finally:
            if writer is not None:
                writer.close()

        # 5) Subir el parquet acumulado en out_buf directamente a S3.
        del in_buf
        out_buf.seek(0)
        S3.put_object(Bucket=S3_BUCKET, Key=out_key, Body=out_buf)
        del out_buf
        gc.collect()

        log.info("MTI %s | written → s3://%s/%s", mti, S3_BUCKET, out_key)


# Thin wrappers that bind each MTI to its fixed subdirectory names.
# To add a new MTI: write a wrapper here and add it to EXTRACTS.


def _extract_1240(client_id: str, file_id: str, file_details: dict, content_hash: str = "") -> None:
    """Extract MTI 1240: 200_IPM_1240_TRA → 300_IPM_1240_EXT."""
    _extract_standard(
        "1240", client_id, file_id, file_details, "200_IPM_1240_TRA", "300_IPM_1240_EXT",
        content_hash=content_hash,
    )


def _extract_1442(client_id: str, file_id: str, file_details: dict, content_hash: str = "") -> None:
    """Extract MTI 1442: 200_IPM_1442_TRA → 300_IPM_1442_EXT."""
    _extract_standard(
        "1442", client_id, file_id, file_details, "200_IPM_1442_TRA", "300_IPM_1442_EXT",
        content_hash=content_hash,
    )


def _extract_1740(client_id: str, file_id: str, file_details: dict, content_hash: str = "") -> None:
    """Extract MTI 1740: 200_IPM_1740_TRA → 300_IPM_1740_EXT."""
    _extract_standard(
        "1740", client_id, file_id, file_details, "200_IPM_1740_TRA", "300_IPM_1740_EXT",
        content_hash=content_hash,
    )


# ==============================================================================
# MTI dispatch map
# ==============================================================================

# Maps each MTI string to its extract function.
# To add a new MTI: write a wrapper above and add it here.
EXTRACTS: dict[str, Any] = {
    "1240": _extract_1240,
    "1442": _extract_1442,
    "1644": _extract_1644,
    "1740": _extract_1740,
}

# ==============================================================================
# Output builder — shared contract with mc_transform.py
# ==============================================================================
 
 
def _build_outputs_for_stepfunction(s3_urls: list[str]) -> list[dict]:
    """
    Convert the list of full S3 URLs written during extraction into the
    structured array consumed by downstream Step Functions states.
 
    Input:  ["s3://bucket/SBSA/MC/300_IPM_1240_EXT/file_type=IN/date=.../xxx.parquet", ...]
    Output: [{"mti": "1240", "s3_key": "SBSA/MC/300_IPM_1240_EXT/file_type=IN/date=.../xxx.parquet"}, ...]
 
    Mirrors mc_transform._build_outputs_for_stepfunction exactly.
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
    AWS Lambda entry point for the Mastercard extraction stage.
 
    Receives the full Step Functions state as the event payload
    (``Payload.$: "$"``).  Identity fields (client_id, file_id, …) are
    present at the event root level; the transform outputs that drive MTI
    detection live under ``$.extract_input.outputs`` as a list of
    ``{"mti": "...", "s3_key": "..."}`` objects — the same structure that
    mc_transform.py produces and mc_interpreter.py produces before it.
 
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
        "extract_inputs": {
            "outputs": [
                {"mti": "1240", "s3_key": "SBSA/MC/200_IPM_1240_TRA/…parquet"},
                {"mti": "1644", "s3_key": "SBSA/MC/200_IPM_1644_TRA/…parquet"},
                ...
            ],
            ...
        },
        ...
    }
 
    Return (flat dict — aligned with mc_transform.py contract)
    ----------------------------------------------------------
    {
        "status":        "SUCCESS" | "ERROR",
        "total_outputs": <int>,
        "total_records": 0,
        "outputs": [
            {"mti": "1240", "s3_key": "SBSA/MC/300_IPM_1240_EXT/…parquet"},
            {"mti": "1644", "s3_key": "SBSA/MC/300_IPM_1644_EXT/…parquet"},
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
    #    Mirrors mc_transform.py field extraction pattern exactly.
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
    # 3. Derive MTIs from extract_input.outputs
    #    Scans the 200_IPM_<MTI>_TRA paths produced by mc_transform,
    #    mirroring how mc_transform derives MTIs from
    #    interpreter_result.outputs by scanning 100_IPM_<MTI>_RAW.
    # ------------------------------------------------------------------
    extract_input = event.get("extract_input", {})
    outputs = extract_input.get("outputs", [])
 
    mtis: list[str] = []
 
    if outputs:
        mtis_from_outputs = list({
            output["mti"]
            for output in outputs
            if output.get("mti") in EXTRACTS
        })
 
        if mtis_from_outputs:
            log.info("MTIs derived from extract_input.outputs: %s", mtis_from_outputs)
            mtis = mtis_from_outputs
        else:
            log.warning(
                "Could not derive MTIs from extract_input.outputs; "
                "falling back to all registered MTIs."
            )
            mtis = list(EXTRACTS.keys())
    else:
        log.info("extract_input.outputs is empty; using all registered MTIs.")
        mtis = list(EXTRACTS.keys())
 
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
    # 5. Run extract pipeline per MTI
    # ------------------------------------------------------------------
    t_global = perf_counter()
    mtis_ok: list[str] = []
 
    for mti in mtis:
        extract_fn = EXTRACTS.get(mti)
        if extract_fn is None:
            log.warning("MTI %s has no registered extract function; skipping", mti)
            continue
 
        log.info("START extract_%s", mti)
        t = perf_counter()
        extract_fn(client_id=client_id, file_id=file_id, file_details=file_details, content_hash=content_hash)
        log.info("END extract_%s | time=%.2fs", mti, perf_counter() - t)
        mtis_ok.append(mti)
 
    log.info(
        "=== Done: %d MTIs processed | total time=%.2fs ===",
        len(mtis_ok),
        perf_counter() - t_global,
    )

    # ------------------------------------------------------------------
    # 6. Collect real output paths written to 300_IPM_*_EXT
    #    Mirrors mc_transform's output collection from 200_IPM_*_TRA.
    # ------------------------------------------------------------------
    uploaded_outputs: list[str] = []
 
    for mti in mtis_ok:
        output_subdir = f"300_IPM_{mti}_EXT"
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
    # 7. Return flat response — aligned with mc_transform.py contract
    #    outputs is a list of {"mti": "...", "s3_key": "..."} objects,
    #    matching the structure produced by mc_transform and mc_interpreter.
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