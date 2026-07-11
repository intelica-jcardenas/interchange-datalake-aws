"""
handler.py — Lambda real: itl-0004-itx-dev-intchg-02-lmbd-mc-clean
================================================================================
Archivo:     lambdas/mastercard/clean/src/handler.py

Cuarta etapa del pipeline Mastercard (tras extract). Lee los Parquets de
extract (capa EXT), castea y normaliza cada columna según definiciones de
dtype declaradas en DynamoDB (mastercard_fields), aplica conversión de
moneda usando la tabla de referencia de decimales por moneda desde S3
(currency/data.parquet), aplica un orden de columnas determinístico y
escribe a s3-staging/400_IPM_{mti}_CLN/ usando un schema PyArrow explícito
(_build_arrow_schema). Equivalente funcional al clean de Visa. Timeout
600s, /tmp 10240 MB (config.json).

El motor de casteo (_cast_df) soporta 6 tipos declarados en DynamoDB:
int64, string, timestamp, date, time, decimal — con 3 variantes de
decimal según el flag float_decimals:
  - >= 0   : decimales implícitos de escala fija (ej. "1234"/scale=2 → 12.34)
  - -1     : formato "scale-prefixed" de Mastercard (primer dígito =
             exponente, resto = mantisa — usado en tipos de cambio)
  - -2/-3/-4: decimales dinámicos, cuya escala depende del código de
             moneda de la propia fila (DE_49/DE_50/DE_51 según el flag) —
             requiere el mapa de decimales por moneda (currency_map)

Como CAL/ITX (Glue) no tienen el mismo control de schema explícito que
esta etapa, el schema Arrow que arma _build_arrow_schema() es la fuente
de verdad que consumen mc-store y consumidores downstream para detectar
degradaciones de tipo (ver decisions.md → "Por qué lmbd-mc-store restaura
el schema Arrow del CLN...").

Cada Parquet se procesa en streaming (iter_batches + ParquetWriter) para
no materializar el DataFrame completo en memoria — mismo patrón que
mc-extract.

Flujo:
1. Derivar los MTIs a procesar desde clean_input.outputs (fallback: todos
   los MTIs registrados en CLEANS)
2. Por cada MTI: listar los Parquets EXT del file_id, procesarlos en
   streaming (castear tipos, construir/reutilizar schema Arrow, escribir)
3. Escribir cada Parquet limpio a 400_IPM_{mti}_CLN/
4. Recolectar los paths reales escritos y construir la lista de outputs
   para la siguiente etapa (glue-mc-calculate)

MTIs soportados: 1240, 1442, 1644 (filtrado por Function Code: 685, 688, 691), 1740

Variables de entorno:
  S3_BUCKET                  : bucket de staging (lectura de EXT, escritura de CLN; default: itl-0004-itx-dev-intchg-02-s3-staging)
  S3_BUCKET_REFERENCE        : bucket de referencia, para currency/data.parquet (default: itl-0004-itx-dev-intchg-02-s3-reference)
  DYNAMO_TABLE_FILE_CONTROL  : tabla de control de archivos (no usada actualmente — file_details se arma desde el evento; default: itl-0004-itx-dev-dynamo-file_control-02)
  ITX_CLEAN_BATCH_SIZE       : filas por batch de streaming (default: 100000)

Estructura de S3 key:
  Entrada (EXT): {client_id}/{brand_id}/{subdir}/file_type={file_type}/date={date}/
  Salida  (CLN): {client_id}/{brand_id}/{subdir}/file_type={file_type}/date={date}/

Referencia de moneda: s3://{S3_BUCKET_REFERENCE}/currency/data.parquet,
cargada una sola vez por proceso y cacheada a nivel de módulo.
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

# Rows read per batch when iterating parquet files with iter_batches.
# Avoids materialising the full decompressed DataFrame in RAM.
CLEAN_BATCH_SIZE = int(os.environ.get("ITX_CLEAN_BATCH_SIZE", "100000"))

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
    """
    Redondea un Decimal a scale dígitos fraccionarios usando ROUND_HALF_UP.

    Args:
        d: Decimal a redondear.
        scale: Cantidad de dígitos fraccionarios deseados.

    Returns:
        Decimal redondeado a scale dígitos fraccionarios.

    Ejemplo:
        _quantize(Decimal("12.345"), 2)  # Decimal("12.35")
    """
    return d.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
 
 
def _to_implied_decimal(x: Any, scale: int) -> Optional[Decimal]:
    """
    Convierte un string numérico a Decimal aplicando decimales implícitos.

    scale=2: "1234" → Decimal("12.34"). Los valores que ya contienen un punto
    decimal se devuelven tal cual, para evitar escalar dos veces. Vacío/NA → None.

    Args:
        x: Valor crudo a convertir (string numérico, o ya con punto decimal).
        scale: Cantidad de decimales implícitos a aplicar si x no tiene punto.

    Returns:
        Decimal convertido, o None si x es NA/vacío o no es un número válido.

    Ejemplo:
        _to_implied_decimal("1234", 2)  # Decimal("12.34")
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
    Parsea un valor numérico con prefijo de escala (formato propio de
    Mastercard) a Decimal.

    Codificación: el primer dígito es el exponente, el resto de los dígitos
    es la mantisa. Ejemplo: "212345" → exponente=2, mantisa=12345 →
    Decimal("123.45"). Vacío/NA → None.

    Args:
        x: Valor crudo a convertir (string con prefijo de escala, o ya con
            punto decimal).
        out_scale: Si se especifica, redondea el resultado a esa cantidad de
            decimales (ROUND_HALF_UP).

    Returns:
        Decimal convertido, o None si x es NA/vacío, tiene menos de 2 dígitos,
        o no es numérico.

    Ejemplo:
        _to_scale_prefixed_decimal("212345")  # Decimal("123.45")
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
    Convierte un string numérico de monto a Decimal usando la cantidad de
    decimales de la moneda de esa fila (per-row currency decimals) — usado
    para columnas cuya escala depende del código de moneda transaccional
    (DE_49/50/51), no de un valor fijo.

    Usa default_decimals como fallback cuando el valor de decimals de la fila
    está ausente. Soporta un signo '-' inicial opcional. Vacío/NA → None.

    Args:
        amount_str: Monto crudo (string de dígitos, con signo '-' opcional).
        decimals: Cantidad de decimales de la moneda de esta fila (de
            currency_map), o None/NA para usar default_decimals.
        default_decimals: Decimales a usar si decimals es None/NA.
        out_scale: Escala de salida a la que redondear el resultado
            (ROUND_HALF_UP).

    Returns:
        Decimal convertido (con signo si aplica), o None si amount_str es
        NA/vacío o no son solo dígitos.

    Ejemplo:
        _to_dynamic_decimal("12345", 2, default_decimals=2, out_scale=4)
        # Decimal("123.4500")
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
    """
    Deserializa un atributo de DynamoDB a un string plano recortado.

    Args:
        attr: Dict de atributo DynamoDB, ej. {"S": "1240"}.

    Returns:
        El valor como string, recortado de espacios. Cadena vacía si el
        atributo no tiene ni "S" ni "N".

    Ejemplo:
        _dval({"S": " 1240 "})  # "1240"
    """
    return str(attr.get("S") or attr.get("N") or "").strip()
 
 
def _get_fields_rows() -> list[dict]:
    """
    Devuelve todas las filas de DYNAMO_TABLE_FIELDS, escaneando DynamoDB solo
    una vez por ciclo de vida del proceso. Usa Scan; la tabla es pequeña, un
    scan completo es aceptable. El resultado se cachea a nivel de módulo.

    Returns:
        Lista de items crudos de DynamoDB (dicts con el envelope de tipo de
        cada atributo).

    Ejemplo:
        _get_fields_rows()  # [{'column_name': {'S': 'amount_transaction_de_4'}, ...}, ...]
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
    Construye y devuelve el DataFrame de definiciones de campo (dtype) para
    la variante solicitada.

    Arma el DataFrame desde el scan cacheado de DynamoDB, antepone las
    columnas base correspondientes, y cachea el resultado por tag.

    Args:
        tag: Llave de caché — usar "default" para 1644/1740 y
            "with_file_cols" para 1240/1442.
        with_file_cols: Si es True, agrega las columnas de metadata a nivel
            de archivo (file_id, file_type, file_processing_date) al set
            base.

    Returns:
        DataFrame con columnas extract_name, data_type, float_decimals — las
        columnas base (_BASE_COLS/_BASE_COLS_WITH_FILE_META) primero, ganan
        en caso de duplicado con las de DynamoDB.

    Ejemplo:
        _load_field_defs("with_file_cols", with_file_cols=True)
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
    Devuelve el mapa código de moneda → cantidad de decimales.

    Carga s3://{S3_BUCKET_REFERENCE}/currency/data.parquet en la primera
    llamada y cachea el resultado para el ciclo de vida del proceso.

    Returns:
        Dict {código_numérico_moneda (3 dígitos, zero-padded): decimales o
        None si currency_decimal_separator está vacío}.

    Ejemplo:
        _get_currency_map()  # {'840': 2, '392': 0, ...}
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
    Castea las columnas de un DataFrame según las definiciones de tipo
    declaradas en DynamoDB (metadata-driven).

    Tipos de data_type soportados: int64, string, timestamp, date, time,
    decimal.

    Flags de float_decimals para columnas decimal:
      - >= 0    : escala de decimales implícitos fija
      - -1      : formato scale-prefixed (tipos de cambio) — ver
        _to_scale_prefixed_decimal
      - -2/-3/-4: decimales dinámicos según el código de moneda de
        DE_49/50/51 de cada fila — ver _to_dynamic_decimal. Requiere
        currency_decimals_map.

    string: además del cast, "pan_de_2" recibe una limpieza adicional
    (cualquier carácter no numérico → '0', máscara de PAN parcial — mismo
    patrón que account_number en el clean de Visa).

    Al final reordena las columnas: las definidas en param primero (en su
    orden), las no declaradas (extras) al final.

    Args:
        df: DataFrame de entrada, de un Parquet de extract.
        param: Tabla de metadata con columnas extract_name, data_type, y
            opcionalmente float_decimals.
        date_format: Formato strptime para columnas date (default: "%Y%m%d").
        timestamp_format: Formato strptime para columnas timestamp; si es
            None, pandas infiere.
        default_decimal_scale: Escala de decimales implícitos por defecto,
            usada cuando float_decimals está ausente/NA.
        conversion_rate_scale: Escala de salida para decimales
            scale-prefixed (float_decimals == -1).
        dynamic_decimal_out_scale: Escala de salida para decimales dinámicos
            (float_decimals en {-2, -3, -4}).
        currency_decimals_map: Mapa moneda→decimales ya construido. Requerido
            si el DataFrame tiene columnas de decimal dinámico.

    Returns:
        Nuevo DataFrame con las columnas casteadas, en el orden
        metadata-driven (columnas definidas primero, extras al final).

    Raises:
        ValueError: si hay columnas de decimal dinámico pero no se pasó
            currency_decimals_map.

    Ejemplo:
        _cast_df(df, field_defs, currency_decimals_map=currency_map)
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
            if col == "pan_de_2":
                out[col] = out[col].str.replace(r"\D", "0", regex=True)

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
    Construye un schema PyArrow que coincide con las reglas de casteo
    metadata-driven de _cast_df — usado para que el ParquetWriter escriba con
    tipos físicos consistentes entre archivos y batches.

    Args:
        param: Misma tabla de metadata pasada a _cast_df.
        ordered_cols: Orden final de columnas del schema — debe ser
            list(df_cast.columns) para que el schema coincida exactamente con
            el Parquet que se está escribiendo. Columnas presentes acá pero
            ausentes en metadata caen a pa.string().
        default_decimal_precision: Precisión para todos los campos
            pa.decimal128 (default: 18).
        default_decimal_scale: Escala de fallback cuando float_decimals está
            ausente/NA.
        conversion_rate_scale: Escala para campos decimal scale-prefixed
            (float_decimals == -1).
        timestamp_unit: Unidad de timestamp Arrow (default: "ns").

    Returns:
        pa.Schema con un campo por columna de ordered_cols, tipado según
        data_type/float_decimals de param.

    Ejemplo:
        _build_arrow_schema(field_defs, ordered_cols=list(df_cast.columns))
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
    Recupera la metadata de un archivo desde la tabla DynamoDB file_control,
    vía get_item. No usada actualmente en el flujo principal del handler (que
    arma file_details directamente desde el evento — ver paso 4 de
    lambda_handler) — queda disponible para reprocesos manuales.

    Args:
        client_id: Código de cliente esperado, para validar que el registro
            encontrado le pertenece.
        file_id: Identificador del archivo (llave de partición de
            file_control).

    Returns:
        Dict con brand_id, file_type, file_processing_date,
        landing_file_name.

    Raises:
        ValueError: si no existe un registro para file_id, o si el
            client_id del registro no coincide con el esperado.

    Ejemplo:
        _get_file_details("SBSA", "DD9D...")
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
    Construye el prefix de S3 key para un cliente y subdirectorio dados,
    según el esquema de particionamiento del pipeline.

    Args:
        client_id: Código de cliente, ej. "SBSA".
        subdir: Subdirectorio de staging, ej. "300_IPM_1240_EXT".
        file_details: Dict con brand_id, file_type y file_processing_date.

    Returns:
        Prefix con barra final: "{client_id}/{brand_id}/{subdir}/file_type={file_type}/date={date}/".

    Ejemplo:
        _s3_prefix("SBSA", "400_IPM_1240_CLN", file_details)
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
    Lista los S3 keys de Parquets bajo prefix cuyo nombre de archivo empieza
    con file_id, paginando automáticamente.

    Args:
        prefix: Prefix de S3 donde buscar.
        file_id: Prefijo del nombre de archivo a filtrar.

    Returns:
        Lista de keys que matchean, ordenada por nombre de archivo.

    Ejemplo:
        _list_parquet_keys("SBSA/MC/300_IPM_1240_EXT/file_type=IN/date=2026-02-18/", "DD9D...")
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
    """
    Descarga un Parquet de S3 y lo devuelve como DataFrame de pandas.

    Args:
        key: S3 key del Parquet a leer.

    Returns:
        DataFrame con el contenido del Parquet.

    Ejemplo:
        _read_parquet("SBSA/MC/300_IPM_1240_EXT/.../x.parquet")
    """
    body = S3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def _align_df_to_schema(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """
    Selecciona las columnas del schema desde df, coacciona los dtypes de
    pandas para que coincidan con los tipos Arrow, y devuelve una pa.Table
    lista para ParquetWriter.write_table().

    Se llama una vez por batch dentro del loop de iter_batches — sin estado,
    independiente entre filas. Las columnas del schema ausentes en el chunk
    actual (ej. un padre PDS que fue null en todas las filas de este Parquet
    pero presente en uno anterior que armó el schema) se rellenan con nulls,
    para que el schema se mantenga consistente entre todos los archivos de
    salida.

    Args:
        df: DataFrame ya casteado (salida de _cast_df).
        schema: Schema Arrow objetivo (de _build_arrow_schema).

    Returns:
        pa.Table con exactamente las columnas del schema, en su orden, y
        tipos coaccionados (string/int64/int32/date32 según el campo).

    Ejemplo:
        _align_df_to_schema(df_cast, schema)
    """
    schema_cols = [f.name for f in schema]
    df_aligned = df[[c for c in schema_cols if c in df.columns]].copy()

    for field in schema:
        col = field.name
        if col not in df_aligned.columns:
            # Column absent in this chunk (e.g. PDS parent was null for all rows
            # in this parquet but present in an earlier one that built the schema).
            # Fill with nulls so the schema stays consistent across all output files.
            df_aligned[col] = None
            continue
        if pa.types.is_string(field.type):
            df_aligned[col] = df_aligned[col].astype("string")
        elif pa.types.is_int64(field.type):
            df_aligned[col] = pd.to_numeric(df_aligned[col], errors="coerce").astype("Int64")
        elif pa.types.is_int32(field.type):
            df_aligned[col] = pd.to_numeric(df_aligned[col], errors="coerce").astype("Int32")
        elif pa.types.is_date(field.type):
            # Native Python date objects — the only representation PyArrow reliably
            # writes as date32 across Lambda's older pyarrow release.
            dt_series = pd.to_datetime(df_aligned[col], errors="coerce")
            df_aligned[col] = [None if pd.isna(v) else v.date() for v in dt_series]

    return pa.Table.from_pandas(df_aligned, schema=schema, preserve_index=False)


def _write_parquet_with_schema(df: pd.DataFrame, key: str, schema: pa.Schema) -> None:
    """
    Serializa un DataFrame completo como Parquet usando un schema PyArrow y
    lo sube a S3 (escritura no incremental — usada solo fuera del hot path
    principal, que escribe con ParquetWriter en streaming). Solo se escriben
    las columnas presentes tanto en df como en schema, garantizando un
    archivo de salida schema-conformante.

    Args:
        df: DataFrame a escribir.
        key: S3 key de destino.
        schema: Schema Arrow a aplicar.

    Returns:
        None.

    Ejemplo:
        _write_parquet_with_schema(df, "SBSA/MC/400_IPM_1240_CLN/.../x.parquet", schema)
    """
    buf = io.BytesIO()
    pq.write_table(_align_df_to_schema(df, schema), buf, compression="snappy")
    buf.seek(0)
    S3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf)


def _target_key(
    raw_key: str, target_prefix: str, mti: str, fc: str | None = None
) -> str:
    """
    Deriva el S3 key de destino a partir del key de origen, cambiando el
    nombre de archivo según el patrón esperado:
      - MTI 1644 con Function Code (fuera de RAW): {md5}_{file_idn}_{mti}_{fc}.parquet
      - Resto de los casos:                        {md5}_{file_idn}_{mti}.parquet

    Args:
        raw_key: S3 key de origen (dentro de una carpeta *_EXT).
        target_prefix: Prefix de destino (carpeta *_CLN).
        mti: MTI del archivo, ej. "1240" o "1644".
        fc: Function Code (solo relevante para MTI 1644 fuera de RAW).

    Returns:
        S3 key completo de destino.

    Raises:
        ValueError: si el stem del archivo de origen no matchea ninguno de
            los 2 patrones esperados.

    Ejemplo:
        _target_key("SBSA/MC/300_IPM_1644_EXT/.../HASH_FILEIDN25CHARS_1644_685.parquet",
                    "SBSA/MC/400_IPM_1644_CLN/.../", mti="1644", fc="685")
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
    Limpia y estandariza los Parquets MTI 1644 de un archivo, uno por
    Function Code. Por cada Parquet: deriva el FC del nombre de archivo,
    descarta FCs no soportados (VALID_FC_1644), procesa en streaming
    (iter_batches), castea columnas (_cast_df) y escribe el resultado a
    400_IPM_1644_CLN/{fc}.parquet.

    El schema Arrow se reconstruye por archivo (no se reutiliza entre FCs)
    porque cada Function Code puede producir un conjunto de columnas
    distinto.

    Args:
        client_id: Código de cliente.
        file_id: Identificador del archivo origen.
        file_details: Dict con brand_id, file_type, file_processing_date.
        origin_sub_dir: Subdirectorio de origen (default: "300_IPM_1644_EXT").
        target_sub_dir: Subdirectorio de destino (default: "400_IPM_1644_CLN").
        content_hash: MD5 del archivo origen (no usado directamente en esta
            función — ya viene propagado como columna desde extract).

    Returns:
        None — escribe los Parquets limpios directamente a S3.

    Ejemplo:
        _clean_1644("SBSA", "DD9D...", file_details)
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

        # ── Lectura en streaming: nunca se materializa el DataFrame completo ──
        # 1) Descargar bytes comprimidos y crear un buffer seekable.
        body = S3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        in_buf = io.BytesIO(body)
        del body
        gc.collect()

        pf = pq.ParquetFile(in_buf)
        out_key = _target_key(key, target_prefix, mti="1644", fc=fc)
        out_buf = io.BytesIO()
        writer = None
        schema = None  # reconstruido por archivo (cada FC produce columnas distintas)

        try:
            # 2) Iterar de a CLEAN_BATCH_SIZE filas — nunca el DataFrame entero.
            for batch in pf.iter_batches(batch_size=CLEAN_BATCH_SIZE):
                df = batch.to_pandas()
                df_cast = _cast_df(df=df, param=field_defs, currency_decimals_map=currency_map)
                del df

                # 3) Construir el schema Arrow en el primer batch y reutilizarlo.
                if schema is None:
                    schema = _build_arrow_schema(
                        field_defs,
                        ordered_cols=list(df_cast.columns),
                        default_decimal_precision=18,
                        default_decimal_scale=2,
                        timestamp_unit="ns",
                    )

                table = _align_df_to_schema(df_cast, schema)
                if writer is None:
                    writer = pq.ParquetWriter(out_buf, schema, compression="snappy")
                writer.write_table(table)

                del df_cast, table
                gc.collect()
        finally:
            if writer is not None:
                writer.close()

        # 4) Subir el parquet acumulado directamente a S3.
        del in_buf
        out_buf.seek(0)
        S3.put_object(Bucket=S3_BUCKET, Key=out_key, Body=out_buf)
        del out_buf
        gc.collect()

        log.info("MTI 1644 clean | written → s3://%s/%s", S3_BUCKET, out_key)


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
    Pipeline de limpieza compartido para los MTIs 1240, 1442 y 1740. Por cada
    Parquet del archivo: procesa en streaming (iter_batches), castea columnas
    (_cast_df) y escribe el resultado a {target_sub_dir}/.

    El schema Arrow se construye desde el primer batch del primer archivo y
    se reutiliza para todos los archivos siguientes del mismo MTI dentro de
    la misma invocación, ya que todos comparten la misma estructura de
    columnas.

    Args:
        mti: MTI a procesar, ej. "1240".
        client_id: Código de cliente.
        file_id: Identificador del archivo origen.
        file_details: Dict con brand_id, file_type, file_processing_date.
        origin_sub_dir: Subdirectorio de origen, ej. "300_IPM_1240_EXT".
        target_sub_dir: Subdirectorio de destino, ej. "400_IPM_1240_CLN".
        date_format: Formato strptime para columnas date (default: "%y%m%d").
        timestamp_format: Formato strptime para columnas timestamp (default:
            "%y%m%d%H%M%S").
        field_defs_tag: Llave de caché para _load_field_defs ("default" o
            "with_file_cols").
        with_file_cols: Si True, agrega las columnas de metadata a nivel de
            archivo al layout base (1240/1442).
        content_hash: MD5 del archivo origen (no usado directamente en esta
            función — ya viene propagado como columna desde extract).

    Returns:
        None — escribe los Parquets limpios directamente a S3.

    Ejemplo:
        _clean_standard("1240", "SBSA", "DD9D...", file_details,
                         "300_IPM_1240_EXT", "400_IPM_1240_CLN",
                         field_defs_tag="with_file_cols", with_file_cols=True)
    """
    origin_prefix = _s3_prefix(client_id, origin_sub_dir, file_details)
    target_prefix = _s3_prefix(client_id, target_sub_dir, file_details)
    list_keys = _list_parquet_keys(origin_prefix, file_id)
    log.info("MTI %s clean | %d files under %s", mti, len(list_keys), origin_prefix)
 
    field_defs = _load_field_defs(field_defs_tag, with_file_cols=with_file_cols)
    currency_map = _get_currency_map()
 
    # Schema is built once from the first batch of the first file and reused —
    # all files within the same MTI share the same column structure.
    schema: Optional[pa.Schema] = None

    for key in list_keys:
        # ── Lectura en streaming: nunca se materializa el DataFrame completo ──
        # 1) Descargar bytes comprimidos y crear un buffer seekable.
        body = S3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        in_buf = io.BytesIO(body)
        del body
        gc.collect()

        pf = pq.ParquetFile(in_buf)
        out_key = _target_key(key, target_prefix, mti=mti)
        out_buf = io.BytesIO()
        writer = None

        try:
            # 2) Iterar de a CLEAN_BATCH_SIZE filas — nunca el DataFrame entero.
            for batch in pf.iter_batches(batch_size=CLEAN_BATCH_SIZE):
                df = batch.to_pandas()
                df_cast = _cast_df(
                    df=df,
                    param=field_defs,
                    date_format=date_format,
                    timestamp_format=timestamp_format,
                    currency_decimals_map=currency_map,
                )
                del df

                # 3) Construir el schema Arrow en el primer batch y reutilizarlo
                #    en todos los archivos del MTI (misma estructura de columnas).
                if schema is None:
                    schema = _build_arrow_schema(
                        field_defs,
                        ordered_cols=list(df_cast.columns),
                        default_decimal_precision=18,
                        default_decimal_scale=2,
                        timestamp_unit="ns",
                    )

                table = _align_df_to_schema(df_cast, schema)
                if writer is None:
                    writer = pq.ParquetWriter(out_buf, schema, compression="snappy")
                writer.write_table(table)

                del df_cast, table
                gc.collect()
        finally:
            if writer is not None:
                writer.close()

        # 4) Subir el parquet acumulado directamente a S3.
        del in_buf
        out_buf.seek(0)
        S3.put_object(Bucket=S3_BUCKET, Key=out_key, Body=out_buf)
        del out_buf
        gc.collect()

        log.info("MTI %s clean | written → s3://%s/%s", mti, S3_BUCKET, out_key)


# Thin wrappers that bind each MTI to its subdirectories and field-def variant.
# To add a new MTI: write a wrapper here and add it to CLEANS.
 
 
def _clean_1240(client_id: str, file_id: str, file_details: dict, content_hash: str = "") -> None:
    """
    Wrapper de _clean_standard para MTI 1240: 300_IPM_1240_EXT → 400_IPM_1240_CLN.

    Args:
        client_id: Código de cliente.
        file_id: Identificador del archivo origen.
        file_details: Dict con brand_id, file_type, file_processing_date.
        content_hash: MD5 del archivo origen.

    Returns:
        None.

    Ejemplo:
        _clean_1240("SBSA", "DD9D...", file_details)
    """
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
    """
    Wrapper de _clean_standard para MTI 1442: 300_IPM_1442_EXT → 400_IPM_1442_CLN.

    Args:
        client_id: Código de cliente.
        file_id: Identificador del archivo origen.
        file_details: Dict con brand_id, file_type, file_processing_date.
        content_hash: MD5 del archivo origen.

    Returns:
        None.

    Ejemplo:
        _clean_1442("SBSA", "DD9D...", file_details)
    """
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
    """
    Wrapper de _clean_standard para MTI 1740: 300_IPM_1740_EXT → 400_IPM_1740_CLN.

    Args:
        client_id: Código de cliente.
        file_id: Identificador del archivo origen.
        file_details: Dict con brand_id, file_type, file_processing_date.
        content_hash: MD5 del archivo origen.

    Returns:
        None.

    Ejemplo:
        _clean_1740("SBSA", "DD9D...", file_details)
    """
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
    Convierte la lista de URLs S3 completas escritas durante la limpieza en
    el array estructurado que consumen los estados downstream de Step
    Functions. Replica exactamente la lógica de
    mc_extract._build_outputs_for_stepfunction.

    Args:
        s3_urls: Lista de URLs completas ("s3://bucket/key") de los Parquets
            escritos.

    Returns:
        Lista de dicts {"mti": ..., "s3_key": ...}, con "mti"="UNKNOWN" si el
        path no matchea el patrón esperado.

    Ejemplo:
        _build_outputs_for_stepfunction(["s3://bucket/SBSA/MC/400_IPM_1240_CLN/.../x.parquet"])
        # [{'mti': '1240', 's3_key': 'SBSA/MC/400_IPM_1240_CLN/.../x.parquet'}]
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
    Punto de entrada de la Lambda lmbd-mc-clean. Invocada por la Step
    Function Mastercard tras lmbd-mc-extract. Recibe el estado completo de
    Step Functions como payload (Payload.$: "$") — los campos de identidad
    (client_id, file_id, ...) están en la raíz del evento, y los outputs de
    extract que determinan qué MTIs procesar viven bajo
    $.clean_input.outputs (misma estructura que produce mc_extract.py).
    Deriva los MTIs a procesar desde esos outputs (fallback: todos los MTIs
    registrados en CLEANS si no se puede derivar ninguno), arma file_details
    directamente desde el evento (sin round-trip a DynamoDB), ejecuta el
    clean de cada MTI con la función registrada en CLEANS, y recolecta los
    paths reales escritos a 400_IPM_*_CLN para construir el payload de
    salida — mismo contrato que mc_extract.py.

    Args:
        event: Payload de Step Functions con client_id, file_id, brand,
            brand_id, file_type, file_date, content_hash, filename, y
            clean_input.outputs (lista de outputs de extract):
            ```
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
            ```
        context: Contexto de ejecución de Lambda; se usa
            context.aws_request_id para logging.

    Returns:
        Dict con status ("SUCCESS" si se escribió al menos un output, "ERROR"
        si no), total_outputs, total_records (siempre 0), outputs (lista
        {"mti", "s3_key"} de los Parquets escritos) y los campos de identidad
        heredados del evento. Lanza ValueError si falta S3_BUCKET,
        client_id/file_id, o no se pudo derivar ningún MTI a procesar.
        Ejemplo:
        ```
        {
            "status":        "SUCCESS",
            "total_outputs": <int>,
            "total_records": 0,
            "outputs": [
                {"mti": "1240", "s3_key": "SBSA/MC/400_IPM_1240_CLN/…parquet"},
                {"mti": "1644", "s3_key": "SBSA/MC/400_IPM_1644_CLN/…parquet"},
                ...
            ],
            "client_id": "SBSA", "file_id": "DD9D...", "brand": "MASTERCARD",
            "brand_id": "MC", "file_type": "IN", "file_date": "2026-02-18",
            "content_hash": "...", "filename": "...",
        }
        ```

    Ejemplo:
        lambda_handler({'client_id': 'SBSA', 'file_id': 'DD9D...', 'brand_id': 'MC',
                         'file_type': 'IN', 'file_date': '2026-02-18',
                         'clean_input': {'outputs': [...]}}, context)
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