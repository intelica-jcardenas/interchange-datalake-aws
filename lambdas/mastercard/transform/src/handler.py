"""
Core de transformaciones auxiliares para Mastercard transform.
 
Dependency rules
----------------
Capa de LÓGICA PURA:
- No instancia ni importa infraestructura en runtime.
- Database se importa SOLO bajo TYPE_CHECKING (type hints); sin dependencia
  en tiempo de ejecución.
- No importa ningún módulo vecino (mc_extract, mc_extract_core).
- Recibe los layout dicts (dict_de, dict_pds) como parámetros desde el
  orquestador mc_transform.py, que es quien instancia Database y llama
  load_layout_from_db.
- Todas las constantes y helpers de configuración estática que necesita
  se declaran directamente aquí (independencia total de módulos vecinos).
 
Contenido
---------
1.  Tipos
2.  Constantes de layout estáticas por MTI
    2a. BASE_COLS_* y TUPLE_DE_PDS_LYT_*
    2b. Constantes de negocio 1644
    2c. get_base_cols_and_containers()
3.  DB-driven layout loader
    load_layout_from_db() / invalidate_layout_cache()
4.  Fixed-width helpers
5.  DE / subfield helpers
6.  PDS helpers
    6a. Helpers de parseo TLV
    6b. Helpers de negocio 1644
    6c. Pipelines PDS por MTI
"""

from __future__ import annotations
 
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
 
 
from time import perf_counter
 
from collections import defaultdict
from typing import  Dict, Iterable, Union, cast
from enum import StrEnum, auto
import boto3
 
from pathlib import Path
 
import gc
 
from boto3.dynamodb.conditions import Key
 
import os
import io
import json
 
import pandas as pd
import logging
import os
import sys
import re

LOG_LEVEL = os.environ.get("ITX_LOG_LEVEL", "INFO").upper()
 
_MTI_FROM_KEY_RE = re.compile(r"/\d+_IPM_(\d{4})_\w+/")
 
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)
        
class Database:
    """
    Class to handle DynamoDB read operations.
    """
 
    def __init__(self):
        self.dynamodb = boto3.resource("dynamodb")
 
        self.client_table_name = os.environ.get(
            "DDB_CLIENT_TABLE",
            "itl-000|4-itx-dev-dynamo-client-02"
        )
 
        self.file_control_table_name = os.environ.get(
            "DDB_FILE_CONTROL_TABLE",
            "itl-0004-itx-dev-dynamo-file_control-02"
        )
 
        self.mastercard_fields_table_name = os.environ.get(
            "DDB_MASTERCARD_FIELDS_TABLE",
            "itl-0004-itx-dev-dynamo-mastercard_fields-02"
        )

    def read_records(
        self,
        table_name: str,
        fields: list[str],
        where: dict | None = None,
    ) -> pd.DataFrame:
 
        where = where or {}
 
        if table_name == "client":
            if "client_id" not in where:
                raise ValueError("Falta client_id para consultar tabla client")
 
            table = self.dynamodb.Table(self.client_table_name)
 
            response = table.get_item(
                Key={
                    "client_id": where["client_id"]
                }
            )
 
        elif table_name == "file_control":
            if "file_id" not in where:
                raise ValueError("Falta file_id para consultar tabla file_control")
 
            table = self.dynamodb.Table(self.file_control_table_name)
 
            response = table.get_item(
                Key={
                    "file_id": where["file_id"]
                }
            )
 
        else:
            raise ValueError(f"Tabla no soportada: {table_name}")
 
        item = response.get("Item")
 
        if not item:
            return pd.DataFrame(columns=fields)
 
        return pd.DataFrame(
            [[item.get(field) for field in fields]],
            columns=fields
        )

    def get_layout_by_mti(
        self,
        mti: str,
    ) -> tuple[dict, dict]:
 
        if mti in _layout_cache:
            return _layout_cache[mti]
 
        table = self.dynamodb.Table(self.mastercard_fields_table_name)
 
        groups: dict[tuple[str, int], list[tuple[int, int]]] = defaultdict(list)
 
        for type_record in ("DE", "PDS"):
 
            items = []
            query_kwargs = {
                "KeyConditionExpression": Key("type_record").eq(type_record)
            }
 
            while True:
 
                response = table.query(**query_kwargs)
 
                items.extend(response.get("Items", []))
 
                if "LastEvaluatedKey" not in response:
                    break
 
                query_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
 
            for item in items:
                type_mti = str(item.get("type_mti", "")).strip()
 
                if type_mti:
                    valid_mtis = [
                        x.strip()
                        for x in type_mti.split(",")
                        if x.strip()
                    ]
 
                    if mti not in valid_mtis:
                        continue
 
                tag = int(item["tag"])
                subfield = int(item["subfield"])
                length = int(item["length"])
 
                groups[(type_record, tag)].append(
                    (subfield, length)
                )
 
        dict_de: dict = {}
        dict_pds: dict = {}
 
        for (type_record, tag), entries in sorted(
            groups.items(),
            key=lambda x: (x[0][0], x[0][1])
        ):
            field_key = f"{type_record}_{tag}"
            target = dict_de if type_record == "DE" else dict_pds
 
            sub_entries = [
                (subfield, length)
                for subfield, length in entries
                if subfield != 0
            ]
 
            top_entries = [
                (subfield, length)
                for subfield, length in entries
                if subfield == 0
            ]
 
            if sub_entries:
                target[field_key] = {
                    f"{type_record}_{tag}_{subfield}": length
                    for subfield, length in sorted(sub_entries)
            }
 
            elif top_entries:
                target[field_key] = top_entries[0][1]
 
        _layout_cache[mti] = (dict_de, dict_pds)
 
        logging.info(
            f"Layout MTI={mti}: "
            f"{len(dict_de)} DE, {len(dict_pds)} PDS"
        )
 
        return dict_de, dict_pds
  
class _Layer(StrEnum):
    """
    Enum of file storage layers available.
    """
    LANDING = auto()
    STAGING = auto()
    OPERATIONAL = auto()
    REFERENCE = auto()
    
class FileStorage:
    """
    Class to handle all file I/O operations.
    """
    Layer = _Layer
 
    def __init__(self) -> None:
        self.s3 = boto3.client("s3")
 
    def _get_bucket_by_layer(self, layer: _Layer) -> str:
        if layer == self.Layer.LANDING:
            return os.environ.get("S3_LANDING_BUCKET", "itl-0004-itx-dev-poc-02-landing")
 
        if layer == self.Layer.STAGING:
            return os.environ.get("S3_STAGING_BUCKET","itl-0004-itx-dev-poc-02-staging")
 
        if layer == self.Layer.OPERATIONAL:
            return os.environ.get("S3_OPERATIONAL_BUCKET","itl-0004-itx-dev-poc-02-operational")
 
        if layer == self.Layer.REFERENCE:
            return os.environ.get("S3_REFERENCE_BUCKET","itl-0004-itx-dev-poc-02-reference")
 
        raise ValueError(f"No existe bucket configurado para layer={layer}")

    def _get_file_details(self, client_id: str, file_id: str):
        db = Database()
 
        df = db.read_records(
            table_name="file_control",
            fields=[
                "file_processing_date",
                "landing_file_name",
                "file_type",
            ],
            where={
                "client_id": client_id,
                "file_id": file_id,
            },
        )
 
        if df.empty:
            raise ValueError(f"No se encontró file_id={file_id}")
 
        return df.iloc[0]

    def _build_key(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        file_type: str,
        subdir: str = "",
        filename: str | None = None,
    ) -> str:
 
        file_details = self._get_file_details(client_id, file_id)
 
        processing_date = str(file_details["file_processing_date"])
        landing_file_name = file_details["landing_file_name"]
 
        if layer == self.Layer.LANDING:
            return f"{client_id}/{landing_file_name}"
 
        if layer == self.Layer.REFERENCE:
            return f"{subdir}/{filename}"
 
        if not filename:
            raise ValueError("filename es obligatorio para STAGING/OPERATIONAL")
 
        #return f"{client_id}/MC/"f"date={processing_date}/"f"process={subdir}/{filename}"
        return f"{client_id}/MC/{subdir}/file_type={file_type}/date={processing_date}/{filename}"

    def list_parquet_files(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        file_type:str,
        subdir: str,
    ) -> list[str]:
 
        bucket = self._get_bucket_by_layer(layer)
 
        file_details = self._get_file_details(client_id, file_id)
        processing_date = str(file_details["file_processing_date"])
 
        #prefix = f"{client_id}/MC/date={processing_date}/process={subdir}/"
        prefix = f"{client_id}/MC/{subdir}/file_type={file_type}/date={processing_date}/"
 
        logging.info(f"Bucket: {bucket}")
        logging.info(f"Prefix: {prefix}")
 
        paginator = self.s3.get_paginator("list_objects_v2")
 
        keys: list[str] = []
 
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key.rsplit("/", 1)[-1]

                if name.startswith(file_id) and name.endswith(".parquet"):
                    keys.append(key)

        return keys

    def get_landing_object(
        self,
        client_id: str,
        file_id: str,
        file_type: str,
    ) -> tuple[str, str]:
 
        bucket = self._get_bucket_by_layer(self.Layer.LANDING)
 
        key = self._build_key(
            layer=self.Layer.LANDING,
            client_id=client_id,
            file_id=file_id,
            file_type=file_type,
        )
 
        return bucket, key

    def write_parquet(
        self,
        df: pd.DataFrame,
        layer: _Layer,
        client_id: str,
        file_id: str,
        file_type: str,
        subdir: str = "",
        filename: str = "data.parquet",
    ) -> str:
 
        bucket = self._get_bucket_by_layer(layer)
 
        key = self._build_key(
            layer=layer,
            client_id=client_id,
            file_id=file_id,
            file_type=file_type,
            subdir=subdir,
            filename=filename,
        )
 
        tmp_path = f"/tmp/{filename}"
 
        df.to_parquet(
        tmp_path,
        index=False,
        engine="pyarrow",
    )
 
        self.s3.upload_file(
            Filename=tmp_path,
            Bucket=bucket,
            Key=key,
        )
 
        Path(tmp_path).unlink(missing_ok=True)
        
        return f"s3://{bucket}/{key}"

    def read_parquet(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        filename: str = "data.parquet",
    ) -> pd.DataFrame:
 
        bucket = self._get_bucket_by_layer(layer)
 
        key = self._build_key(
            layer=layer,
            client_id=client_id,
            file_id=file_id,
            subdir=subdir,
            filename=filename,
        )
 
        response = self.s3.get_object(
            Bucket=bucket,
            Key=key,
        )
 
        return pd.read_parquet(
            io.BytesIO(response["Body"].read())
        )

    def read_parquet_by_key(
        self,
        layer: _Layer,
        key: str,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
 
        bucket = self._get_bucket_by_layer(layer)
 
        logging.info(f"Leyendo parquet: s3://{bucket}/{key}")
 
        response = self.s3.get_object(
            Bucket=bucket,
            Key=key,
        )
 
        return pd.read_parquet(
            io.BytesIO(response["Body"].read()),
            columns=columns,
        )

# ──────────────────────────────────────────────────────────────────────
# Helper: convierte URLs S3 del interpreter al formato que espera
# mc_calculate → [{"mti": "1240", "s3_key": "SBSA/MC/100_IPM_1240_RAW/..."}]
# ──────────────────────────────────────────────────────────────────────


def _build_outputs_for_stepfunction(s3_urls: list[str]) -> list[dict]:
    """
    Transforma la lista de URLs completas que produce _pipeline_mc_interpreter
    en el array de objetos que consume mc_calculate como parámetro --outputs.
 
    Input:  ["s3://bucket/SBSA/MC/100_IPM_1240_RAW/file_type=IN/date=.../xxx.parquet", ...]
    Output: [{"mti": "1240", "s3_key": "SBSA/MC/100_IPM_1240_RAW/file_type=IN/date=.../xxx.parquet"}, ...]
    """
    result = []
    for url in s3_urls:
        # Quitar prefijo "s3://bucket/" para quedarse solo con el s3_key
        if url.startswith("s3://"):
            # "s3://bucket/rest/of/key" → "rest/of/key"
            without_scheme = url[5:]                          # "bucket/rest/of/key"
            s3_key = without_scheme.split("/", 1)[1]          # "rest/of/key"
        else:
            s3_key = url
 
        m = _MTI_FROM_KEY_RE.search(url)
        mti = m.group(1) if m else "UNKNOWN"
 
        result.append({"mti": mti, "s3_key": s3_key})
    return result


# ==============================================================================
# 1. Tipos
# ==============================================================================
 
PdsLayout = Dict[str, Union[int, Dict[str, int]]]
 
 
# ==============================================================================
# 2. Constantes de layout estáticas por MTI
#    (declaradas aquí para que mc_transform_core sea completamente autónomo;
#     mc_extract_core tiene sus propias copias independientes)
# ==============================================================================
 
# ------------------------------------------------------------------------------
# 2a. BASE_COLS y TUPLE_DE_PDS por MTI
# ------------------------------------------------------------------------------

# MTI 1240 -------------------------------------------------------------------
TUPLE_DE_PDS_LYT_1240 = ("DE_48", "DE_62", "DE_123", "DE_124", "DE_125")
 
BASE_COLS_1240 = [
    "FILE_IDN",
    "FILE_DT",
    "MTI",
    "MSG_NO",
    "FUNCTION_CODE",
]

# MTI 1442 -------------------------------------------------------------------
TUPLE_DE_PDS_LYT_1442 = ("DE_48", "DE_62", "DE_123", "DE_124", "DE_125")
 
BASE_COLS_1442 = [
    "FILE_IDN",
    "FILE_DT",
    "MTI",
    "MSG_NO",
    "FUNCTION_CODE",
]

# MTI 1644 -------------------------------------------------------------------
TUPLE_DE_PDS_LYT_1644 = ("DE_48",)
 
BASE_COLS_1644 = [
    "FILE_IDN",
    "FILE_DT",
    "MSG_NO",
    "BLOCK",
    "MTI",
    "ENC",
    "FUNCTION_CODE",
    "FUNCTION_ROLE",
    "PARSE_OK",
    "DE_1",
]
 
# MTI 1740 -------------------------------------------------------------------
TUPLE_DE_PDS_LYT_1740 = ("DE_48",)
 
BASE_COLS_1740 = [
    "FILE_IDN",
    "FILE_DT",
    "MSG_NO",
    "MTI",
]

# ------------------------------------------------------------------------------
# 2b. Constantes de negocio específicas del MTI 1644
# ------------------------------------------------------------------------------

# Tags PDS esperados por Function Code — regla estática de negocio
_PDS_TAGS_BY_FC_1644: dict[str, set[int]] = {
    "685": {
        148, 165, 300, 302, 358, 370, 372, 374, 378,
        380, 381, 384, 390, 391, 392, 393, 394, 395, 396,
        400, 401, 402,
    },
    "688": {
        148, 300, 302, 359, 368, 369, 370, 372, 374, 378,
        380, 381, 384, 390, 391, 392, 393, 394, 395, 396,
        400, 401, 402,
    },
    "691": {5, 6, 25, 138, 165, 280},
}

# FC válidos para el pipeline 1644
_VALID_FC_1644 = {"685", "688", "691"}

# ------------------------------------------------------------------------------
# 2c. Accessor: (BASE_COLS, TUPLE_DE_PDS) por MTI
# ------------------------------------------------------------------------------

def get_base_cols_and_containers(mti: str) -> tuple[list[str], tuple]:
    """
    Devuelve (BASE_COLS, TUPLE_DE_PDS) para el MTI indicado.
 
    Configuración estática de negocio; no requiere consulta a BD.
    Usada internamente por filter_df_columns_de y por mc_transform.py
    para obtener container_cols antes de llamar apply_pds_for_mti.
    """
    if mti == "1240":
        return BASE_COLS_1240, TUPLE_DE_PDS_LYT_1240
    if mti == "1442":
        return BASE_COLS_1442, TUPLE_DE_PDS_LYT_1442
    if mti == "1644":
        return BASE_COLS_1644, TUPLE_DE_PDS_LYT_1644
    if mti == "1740":
        return BASE_COLS_1740, TUPLE_DE_PDS_LYT_1740
    raise ValueError(f"get_base_cols_and_containers: MTI no soportado: {mti!r}")


# ==============================================================================
# 3. DB-driven layout loader
#    (copia independiente de la que vive en mc_extract_core;
#     cada pipeline — extract y transform — gestiona su propio caché)
# ==============================================================================
 
# Caché módulo-nivel: mti → (dict_de, dict_pds).
# La BD se consulta una sola vez por MTI durante la vida del proceso.
_layout_cache: dict[str, tuple[dict, dict]] = {}
 
# =============================================================================
# 4. Fixed-width helpers
# =============================================================================

def build_expected_columns(
    mti: str,
    dict_de: dict,
    dict_pds: dict,
) -> list[str]:
 
    base_cols, _ = get_base_cols_and_containers(mti)
 
    rename_map = {
        "MSG_NO": "ref_id",
        "MTI": "type_mti",
    }
 
    cols = []
 
    for col in base_cols:
        cols.append(rename_map.get(col, col))
 
    for de_name, de_spec in dict_de.items():
        cols.append(de_name)
 
        if isinstance(de_spec, dict):
            cols.extend(de_spec.keys())
 
    for pds_name, pds_spec in dict_pds.items():
        cols.append(pds_name)
 
        if isinstance(pds_spec, dict):
            cols.extend(pds_spec.keys())

    cols.extend([
        "file_processing_date",
        "file_id",
        "content_hash",
    ])

    return list(dict.fromkeys(cols))

def align_chunk_to_expected_columns(
    chunk: pd.DataFrame,
    expected_columns: list[str],
) -> pd.DataFrame:
 
    for col in expected_columns:
        if col not in chunk.columns:
            chunk[col] = pd.NA
 
    chunk = chunk[expected_columns]
 
    for col in chunk.columns:
        if col == "ref_id":
            chunk[col] = pd.to_numeric(
                chunk[col],
                errors="coerce",
            ).astype("Int64")
        else:
            chunk[col] = chunk[col].astype("string")
 
    return chunk

def expand_de43(df: pd.DataFrame, col: str = "DE_43") ->  pd.DataFrame:
    """
    Expande el campo DE_43 usando el delimitador '\\' y corte posicional del tail.
 
    Resultado:
        DE_43_1  nombre del comercio
        DE_43_2  calle
        DE_43_3  ciudad
        DE_43_4  código postal  (10 chars)
        DE_43_5  subdivisión    (3 chars)
        DE_43_6  país           (3 chars)
    """
    if df is None or df.empty or col not in df.columns:
        return df
    
    s = df[col].fillna("").astype(str)
    parts = s.str.split("\\", n=3, expand=True, regex=False) # Split en 3 delimitadores: name, street, city, tail
 
    while parts.shape[1] < 4: # Asegura 4 columnas aunque falten
        parts[parts.shape[1]] = ""
 
    name    = parts[0].fillna("")
    street  = parts[1].fillna("")
    city    = parts[2].fillna("")
    tail    = parts[3].fillna("")
 
    tail16  = tail.str.pad(16, side="right").str.slice(0, 16) # Tail debe tener al menos 16 chars para (10,3,3)
    postal  = tail16.str.slice(0, 10)
    subdiv  = tail16.str.slice(10, 13)
    country = tail16.str.slice(13, 16)
 
    out = pd.DataFrame(
        {
            "DE_43_1": name,
            "DE_43_2": street,
            "DE_43_3": city,
            "DE_43_4": postal,
            "DE_43_5": subdiv,
            "DE_43_6": country,
        },
        index=df.index,
    )
 
    for c in out.columns: # Limpieza: rstrip y vacíos -> NA
        out[c] = out[c].astype("string").str.rstrip()
        out[c] = out[c].replace("", pd.NA)
 
    to_drop = [c for c in out.columns if c in df.columns] # Si ya existían subfields viejos (mal cortados), los pisamos
    if to_drop:
        df = df.drop(columns=to_drop)
 
    return pd.concat([df, out], axis=1)


def expand_fixed_width_series_to_df(
    serie: pd.Series, 
    spec: dict[str, int], 
    *, 
    prefix: str | None = None,
) -> pd.DataFrame:
    """
    Corta una Serie de texto en subcampos de ancho fijo según `spec`.
 
    Parameters
    ----------
    serie  : pd.Series      Serie de texto de entrada.
    spec   : dict[str, int] Mapping {nombre_subcampo: longitud}.
    prefix : str | None     Prefijo opcional para los nombres de columna.
 
    Returns
    -------
    pd.DataFrame  Un DataFrame con los subcampos como columnas.
    """
    if serie is None or len(serie) == 0:
        return pd.DataFrame(index=getattr(serie, "index", None))
        
    s = serie.fillna("").astype(str) # Normalize string
    mask = s.ne("")
 
    if not mask.any():
        cols = [f"{prefix}{k}" if prefix else k for k in spec.keys()]
        return pd.DataFrame({c: pd.NA for c in cols}, index=serie.index)
    
    s_cut = s.where(mask) # Los vacions vuelven NAN
    out: dict[str, pd.Series] = {}
    pos = 0 
 
    for name, ln in spec.items():
        col_name        = f"{prefix}{name}" if prefix else name # slice vectorizado pero en filas vacias quedara en NaN
        out[col_name]   = s_cut.str.slice(pos, pos + int(ln))
        pos             = pos + int(ln)
 
    df_out = pd.DataFrame(out, index=serie.index)
    return df_out.where(df_out.notna(), pd.NA)


def expand_fixed_width_columns(
        df: pd.DataFrame, 
        specs_by_col: dict[str, dict[str, int]],
        *,
        only_if_present: bool = True,
) -> pd.DataFrame:
    """
    Expande múltiples columnas de ancho fijo y las concatena al DataFrame.
 
    Parameters
    ----------
    df             : pd.DataFrame
    specs_by_col   : dict  {"DE_3": {"DE_3_1": 2, ...}, "PDS_146": {...}}
    only_if_present: bool  Si True, ignora columnas ausentes en df.
 
    Returns
    -------
    pd.DataFrame  df + columnas expandidas.
    """
    if df is None or df.empty or not specs_by_col:
        return df
    
    parts: list[pd.DataFrame] = []
 
    for col, spec in specs_by_col.items(): # si permite ignorar y el col del specs no está en el df = no está en el df
        if only_if_present and col not in df.columns: 
            continue
        s = df[col] # Si esta  vacio al 100% no se expande
        non_empty = s.notna() & (s.astype(str).str.len() > 0)
        if not non_empty.any():
            continue
        sub_df = expand_fixed_width_series_to_df(serie=s, spec=spec)
        parts.append(sub_df)
 
    if not parts:
        return df
    
    sub_all = pd.concat(parts, axis=1)
    return pd.concat([df, sub_all], axis=1)


# ==============================================================================
# 5. DE / subfield helpers
# ==============================================================================
 
def filter_df_columns_de( 
    df: pd.DataFrame, 
    mti: str,
    dict_de: dict,
) -> pd.DataFrame:
    """
    Filtra el DataFrame conservando solo BASE_COLS y las columnas DE del layout.
 
    Parameters
    ----------
    df      : DataFrame de entrada.
    mti     : Tipo de mensaje ("1240", "1442", "1644", "1740").
    dict_de : Diccionario DE para el MTI (cargado desde BD por el orquestador).
    """
    df = df.rename(columns=str.upper)
    base_cols, _ = get_base_cols_and_containers(mti)
 
    cols_to_keep = (
        [c for c in base_cols if c in df.columns] 
        + [c for c in dict_de.keys() if c in df.columns]
    )
    return df[cols_to_keep]


def expand_subfields(
    df: pd.DataFrame, 
    mti: str,
    dict_de: dict,
) -> pd.DataFrame:
    """
    Expande los subcampos fixed-width de los DE cuya spec es un dict.
 
    DE_43 se omite aquí porque tiene regla especial (ver expand_de43).
 
    Parameters
    ----------
    df      : DataFrame de entrada.
    mti     : Tipo de mensaje (para validación de soporte).
    dict_de : Diccionario DE para el MTI (cargado desde BD por el orquestador).
    """
    if df is None or df.empty:
        return df
 
    mapping: dict[str, dict[str, int]] = {}
 
    for de_name, de_spec in dict_de.items():
        if de_name not in df.columns:
            continue
        if de_name == "DE_43":
            continue  # DE_43 se maneja aparte
        if not isinstance(de_spec, dict):
            continue
        mapping[de_name] = cast(dict[str, int], de_spec)
        
    df_out = expand_fixed_width_columns(df=df, specs_by_col=mapping) if mapping else df # primero expandir los fixed-width normales
    df_out = expand_de43(df_out, col="DE_43") # luego expande DE_43 con regla especial
    return df_out


def reorder_with_subfield(
    df: pd.DataFrame, 
    mti: str,
    dict_de: dict,
) -> pd.DataFrame:
    """
    Reordena columnas intercalando cada subcampo justo después de su DE padre.
 
    Parameters
    ----------
    df      : DataFrame de entrada.
    mti     : Tipo de mensaje (para validación de soporte).
    dict_de : Diccionario DE para el MTI (cargado desde BD por el orquestador).
    """
    col_set = set(df.columns)
    cols = []
 
    for c in df.columns:
        cols.append(c)
        spec = dict_de.get(c)
        if isinstance(spec, dict):
            for subc in spec.keys(): # Agregar subcampos si existen
                if subc in col_set:
                    cols.append(subc)
 
    cols = list(dict.fromkeys(cols)) # quitar duplicados manteniendo orden
    return df[cols]


# ==============================================================================
# 6. PDS helpers
# ==============================================================================
 
# ------------------------------------------------------------------------------
# 6a. Helpers de parseo TLV
# ------------------------------------------------------------------------------

def parse_pds_tlv_scan_txt(blob: str, wanted_tag_txt: set[str] ) -> dict[str, str]:
    """
    Parsea un blob PDS TLV con formato: Tag (4 dígitos) + Length (3 dígitos) + Value.
 
    Reglas de parseo
    ----------------
    - Posición inválida (no dígitos)  → avanza +1 char.
    - Length > 999 o desborda el blob → avanza +1 char.
    - TLV válido y tag en wanted      → guarda el valor.
    - TLV válido y tag fuera de wanted→ salta length chars (no guarda).
    """
 
    if not blob:
        return {}
    
    n = len(blob)
    out: dict[str, str] = {}
    i = 0
 
    while i + 7 <= n:
        tag_txt = blob[i:i+4]
        len_txt = blob[i+4:i+7]
 
        # Verified if TLV valid appear in this position
        if not (tag_txt.isdigit() and len_txt.isdigit()):
            i = i + 1
            continue
        
        ln = int(len_txt)
        # Verified if length is valid or not ( jump +1 char)
        if ln > 999:
            i = i + 1
            continue
 
        start_val = i + 7
        end_val = start_val + ln
 
        # Verified if end TLV es valid or not (jump +1 char)
        if end_val > n:
            i = i + 1 
            continue
        
        # TLV valid
        if tag_txt in wanted_tag_txt:
            out[f"PDS_{int(tag_txt)}"] = blob[start_val:end_val]
 
        # Success saved TLV: Jump TLV lengh
        i = end_val
 
    return out

def extract_pds_columns_from_containers_fast(
    df: pd.DataFrame, 
    *, 
    container_cols: Iterable[str], 
    wanted_tags: set[int],
) -> pd.DataFrame:
    """
    Extrae columnas PDS_<tag> desde columnas contenedoras (DE_48, DE_62, …).
 
    Parameters
    ----------
    df             : DataFrame de entrada.
    container_cols : Columnas donde puede aparecer TLV (DE_48, DE_62, …).
    wanted_tags    : Tags numéricos a extraer ({148, 358, …}).
    """
    if df is None or df.empty or not wanted_tags:
        return df
    
    wanted_tag_txt = {f"{t:04d}" for t in wanted_tags}
 
    present_cols = [c for c in container_cols if c in df.columns]
 
    if not present_cols:
        return df
    
    n                                   = len(df)
    cols_with_data: list[str]           = []
    series_cache: dict[str, pd.Series]  = {}
 
    for c in present_cols:
        s = df[c].fillna("").astype(str)
        series_cache[c] = s
        non_empty = (s != "").sum()
        if non_empty > 0:
            cols_with_data.append(c)
 
    if not cols_with_data:
        return df

    # 2) Parser a list 
    parsed_per_col: list[list[dict[str, str]]] = []
 
    for c in cols_with_data:
        blobs = series_cache[c].to_numpy(dtype=object)
 
        # list comprehension 
        parsed = [
            parse_pds_tlv_scan_txt(blob=b, wanted_tag_txt=wanted_tag_txt) if b else {}
            for b in blobs
        ]
        parsed_per_col.append(parsed)
 
    # 3) Merge per row only if have more than 1 container with data
    if len(parsed_per_col) == 1:
        combined = parsed_per_col[0]
    else:
        combined: list[dict[str, str]] = [{} for _ in range(n)]
        for i in range(n):
            d: dict[str, str] = {}
            for col_list in parsed_per_col:
                if col_list[i]:
                    d.update(col_list[i])
            combined[i] = d

    # 4) Expands dicts to columns
    expected_cols   = [f"PDS_{t}" for t in sorted(wanted_tags)]
    pds_df          = pd.DataFrame.from_records(combined)
    pds_df.index    = df.index
    pds_df          = pds_df.reindex(columns=expected_cols)
    pds_df          = pds_df.where(pds_df.notna(), pd.NA)
 
    return pd.concat([df, pds_df], axis=1)


def expand_pds_subfields(
    df: pd.DataFrame, 
    *, 
    pds_layout: PdsLayout
) -> pd.DataFrame:
    """
    Expand subfields of PDS when the layout defined like dictionary.
    """
    if df is None or df.empty:
        return df
    
    # mapping: col -> spec (only when have dict and exists in the df)
    mapping: dict[str, dict[str, int]] = {}
 
    for pds_name, spec in pds_layout.items():
        if not isinstance(spec,dict):
            continue        
        if pds_name not in df.columns:
            continue
        mapping[pds_name] = cast(dict[str, int], spec)
 
    if not mapping:
        return df
        
    return expand_fixed_width_columns(df, mapping)


def wanted_tags_from_layout(pds_layout: dict) -> set[int]:
    """Extrae el conjunto de tags numéricos de un dict PDS layout."""
    return {int(k.split("_")[1]) for k in pds_layout.keys()}

# ------------------------------------------------------------------------------
# 6b. Helpers de negocio 1644
# ------------------------------------------------------------------------------

def wanted_pds_tags_1644(function_code: str, pds_layout: PdsLayout) -> set[int]:
    """
    Devuelve los tags PDS (int) que se deben extraer del DE48 para el FC dado.
 
    Lógica
    ------
    - Si el FC tiene reglas en _PDS_TAGS_BY_FC_1644, las devuelve directamente.
    - Fallback: todos los tags presentes en pds_layout (cargado desde BD).
 
    Parameters
    ----------
    function_code : str       FC del mensaje ("685", "688", "691", …).
    pds_layout    : PdsLayout Layout PDS completo, cargado desde BD por el
                              orquestador y pasado como parámetro.
    """
    fc   = str(function_code) if function_code is not None else ""
    tags = _PDS_TAGS_BY_FC_1644.get(fc)
    if tags:
        return tags
    # Fallback: todos los tags definidos en el layout recibido
    return {int(k.split("_")[1]) for k in pds_layout if k.startswith("PDS_")}

def pds_layout_1644_for_tags(tags: set[int], pds_layout: PdsLayout) -> PdsLayout:
    """
    Filtra pds_layout y devuelve solo las entradas cuyos tags están en `tags`.
 
    Parameters
    ----------
    tags       : set[int]    Tags numéricos a retener.
    pds_layout : PdsLayout   Layout PDS completo, cargado desde BD por el
                             orquestador y pasado como parámetro.
    """
    return {
        k: spec
        for k, spec in pds_layout.items()
        if k.startswith("PDS_") and int(k.split("_")[1]) in tags
    }

# ------------------------------------------------------------------------------
# 6c. Pipelines PDS por MTI
# ------------------------------------------------------------------------------

def apply_pds_for_mti(
    df: pd.DataFrame, 
    *, 
    mti: str,
    dict_pds: dict, 
    container_cols: tuple
) -> pd.DataFrame:
    """
    Pipeline PDS completo para un MTI:
    1) Extrae PDS desde los contenedores TLV.
    2) Expande subcampos de los PDS que tienen spec dict.
 
    Parameters
    ----------
    df             : DataFrame de entrada.
    mti            : Tipo de mensaje (para validación de soporte).
    dict_pds       : Diccionario PDS para el MTI (cargado desde BD por el orquestador).
    container_cols : Tupla de columnas contenedoras de TLV PDS.
                     Obtenida del orquestador vía get_base_cols_and_containers(mti).
    """
    if df is None or df.empty:
        return df
 
    # normalizar columnas a UPPER
    if any(c != c.upper() for c in df.columns):
        df = df.copy()
        df.columns = [c.upper() for c in df.columns]
 
    wanted_tags = wanted_tags_from_layout(dict_pds)
 
    df2 = extract_pds_columns_from_containers_fast(
        df=df,
        container_cols=container_cols,
        wanted_tags=wanted_tags,
    )
 
    df3 = expand_pds_subfields(
        df=df2,
        pds_layout=dict_pds,
    )
 
    return df3


def apply_pds_for_mti_1644_split(
    df: pd.DataFrame,
    *,
    dict_pds_1644: dict,
) -> dict[str, pd.DataFrame]:
    """
    Divide el DataFrame del MTI 1644 por Function Code y aplica el pipeline PDS.
    Devuelve {'685': df_685, '688': df_688, '691': df_691}.
    Si no hay filas para un FC no lo incluye en el resultado.
 
    Parameters
    ----------
    df             : DataFrame de entrada.
    dict_pds_1644  : Diccionario PDS completo del MTI 1644 (cargado desde BD
                     por el orquestador en mc_transform.py).
    """
    if df is None or df.empty:
        return {}
 
    # normalizar columnas a UPPER
    if any(c != c.upper() for c in df.columns):
        df = df.copy()
        df.columns = [c.upper() for c in df.columns]
 
    if "FUNCTION_CODE" not in df.columns:
        return {}
 
    fc_series = df["FUNCTION_CODE"].astype(str)
    df = df[fc_series.isin({"685", "688","691"})]
    if df.empty:
        return {}
 
    out: dict[str, pd.DataFrame] = {}
 
    for fc, g in df.groupby("FUNCTION_CODE", dropna=False):
        fc_str = str(fc)
 
        # Reglas de negocio estáticas: qué tags aplican por FC
        tags = wanted_pds_tags_1644(fc_str, pds_layout=dict_pds_1644) #trae los tags (pds) que se van a usar de acuerdo al function code
 
        # Filtrar el layout por los tags del FC
        pds_layout_fc = pds_layout_1644_for_tags(tags, pds_layout=dict_pds_1644) #traer los pds que se usaran
 
        g2 = extract_pds_columns_from_containers_fast(
            df=g,
            container_cols=TUPLE_DE_PDS_LYT_1644,  # DE_48
            wanted_tags=tags,
        )
        g3 = expand_pds_subfields(df=g2, pds_layout=pds_layout_fc)
        out[fc_str] = g3.sort_index()
    return out

# ==============================================================================
# MTI 1240
# ==============================================================================

def transform_ipm_1240(
    client_id: str,
    file_id: str,
    file_type: str,
    context=None,
    content_hash: str = "",
) -> None:

    t_total = perf_counter()
    db = Database()
 
    file_config = fs._get_file_details(
        client_id=client_id,
        file_id=file_id,
    )
 
    dict_de, dict_pds = db.get_layout_by_mti("1240")
    _, container_cols = get_base_cols_and_containers("1240")
 
    expected_columns = build_expected_columns(
        mti="1240",
        dict_de=dict_de,
        dict_pds=dict_pds,
    )
 
    file_processing_date = file_config["file_processing_date"]
    file_type = file_type
 
    keys = fs.list_parquet_files(
        layer=fs.Layer.STAGING,
        client_id=client_id,
        file_id=file_id,
        file_type=file_type,
        subdir="100_IPM_1240_RAW",
    )
 
    logging.info(f"Total parquets encontrados: {len(keys)}")

    if not keys:
        raise ValueError("No se encontraron parquets para procesar.")
 
    for i, key in enumerate(keys, start=1):
 
        t_file = perf_counter()
        filename = Path(key).name
        tmp_path = f"/tmp/{Path(filename).stem}_{uuid.uuid4().hex}.parquet"
        writer = None
 
        logging.info(f"[{i}/{len(keys)}] Procesando key: {key}")
 
        if context:
            logging.info(
                f"[{i}] Tiempo restante Lambda: "
                f"{context.get_remaining_time_in_millis() / 1000:.2f}s"
            )
 
        try:
            # ============================================================
            # 1) Leer parquet origen
            # ============================================================
            t = perf_counter()
 
            df = fs.read_parquet_by_key(
                layer=fs.Layer.STAGING,
                key=key,
            )
 
            logging.info(
                f"[{i}] read_parquet: {perf_counter() - t:.2f}s | "
                f"rows={len(df)} | cols={len(df.columns)}"
            )
 
            # ============================================================
            # 2) Filtrar columnas DE necesarias
            # ============================================================
            t = perf_counter()
 
            df_de_only = filter_df_columns_de(
                df=df,
                mti="1240",
                dict_de=dict_de,
            )
 
            del df
            gc.collect()
 
            logging.info(
                f"[{i}] filter_df_columns_de: {perf_counter() - t:.2f}s | "
                f"rows={len(df_de_only)} | cols={len(df_de_only.columns)}"
            )

            # ============================================================
            # 3) Calcular chunk_size dinámico
            # ============================================================
            total_rows = len(df_de_only)
 
            memory_mb = (
                df_de_only.memory_usage(deep=True).sum()
                / 1024
                / 1024
            )
 
            target_chunk_mb = 500
 
            chunk_size = max(
                10_000,
                int(total_rows * target_chunk_mb / max(memory_mb, 1)),
            )
 
            
 
            chunk_size = min(chunk_size, 100_000)
 
            total_chunks = (total_rows + chunk_size - 1) // chunk_size
 
            logging.info(
                f"[{i}] memory_df_de_only={memory_mb:.2f} MB | "
                f"chunk_size={chunk_size} | "
                f"total_chunks={total_chunks}"
            )

            # ============================================================
            # 4) Procesar chunks y escribir a un solo parquet local
            # ============================================================
            for chunk_idx, start in enumerate(
                range(0, total_rows, chunk_size),
                start=1,
            ):
 
                t_chunk = perf_counter()
                end = min(start + chunk_size, total_rows)
 
                logging.info(
                    f"[{i}.{chunk_idx}/{total_chunks}] "
                    f"filas={start}:{end}"
                )
 
                chunk = df_de_only.iloc[start:end].copy()
 
                # 4.1 Expandir DE subfields
                t = perf_counter()
 
                chunk = expand_subfields(
                    df=chunk,
                    mti="1240",
                    dict_de=dict_de,
                )
 
                logging.info(
                    f"[{i}.{chunk_idx}] expand_subfields: "
                    f"{perf_counter() - t:.2f}s | "
                    f"rows={len(chunk)} | cols={len(chunk.columns)}"
                )
 
                # 4.2 Reordenar columnas
                t = perf_counter()
 
                chunk = reorder_with_subfield(
                    df=chunk,
                    mti="1240",
                    dict_de=dict_de,
                )
 
                logging.info(
                    f"[{i}.{chunk_idx}] reorder_with_subfield: "
                    f"{perf_counter() - t:.2f}s | "
                    f"rows={len(chunk)} | cols={len(chunk.columns)}"
                )

                # 4.3 Aplicar PDS
                t = perf_counter()
 
                chunk = apply_pds_for_mti(
                    df=chunk,
                    mti="1240",
                    dict_pds=dict_pds,
                    container_cols=container_cols,
                )
 
                logging.info(
                    f"[{i}.{chunk_idx}] apply_pds_for_mti: "
                    f"{perf_counter() - t:.2f}s | "
                    f"rows={len(chunk)} | cols={len(chunk.columns)}"
                )
 
                # 4.4 Rename + metadata
                t = perf_counter()
 
                chunk = chunk.rename(
                    columns={
                        "MSG_NO": "ref_id",
                        "MTI": "type_mti",
                    }
                )
 
                chunk["file_processing_date"] = file_processing_date
                chunk["file_id"] = file_id
                chunk["content_hash"] = content_hash
 
                logging.info(
                    f"[{i}.{chunk_idx}] rename_metadata: "
                    f"{perf_counter() - t:.2f}s | "
                    f"rows={len(chunk)} | cols={len(chunk.columns)}"
                )

                # 4.5 Escribir chunk al mismo parquet local
                t = perf_counter()
 
 
                chunk = align_chunk_to_expected_columns(
                    chunk=chunk,
                    expected_columns=expected_columns,
                )
                
                table = pa.Table.from_pandas(
                    chunk,
                    preserve_index=False,
                )
 
                if writer is None:
                    writer = pq.ParquetWriter(
                        tmp_path,
                        table.schema,
                        compression="snappy",
                    )
 
                writer.write_table(table)
 
                logging.info(
                    f"[{i}.{chunk_idx}] write_chunk_tmp: "
                    f"{perf_counter() - t:.2f}s | "
                    f"rows={len(chunk)} | cols={len(chunk.columns)}"
                )
 
                del chunk
                del table
                gc.collect()
 
                logging.info(
                    f"[{i}.{chunk_idx}] total_chunk: "
                    f"{perf_counter() - t_chunk:.2f}s"
                )
 
            del df_de_only
            gc.collect()
 
        finally:
            if writer is not None:
                writer.close()

        # ============================================================
        # 5) Subir único parquet final a S3
        # ============================================================
        t = perf_counter()
 
        bucket = fs._get_bucket_by_layer(layer.STAGING)
 
        target_key = fs._build_key(
            layer=layer.STAGING,
            client_id=client_id,
            file_id=file_id,
            file_type =  file_type,
            subdir="200_IPM_1240_TRA",
            filename=filename,
        )
 
        fs.s3.upload_file(
            Filename=tmp_path,
            Bucket=bucket,
            Key=target_key,
        )
 
        Path(tmp_path).unlink(missing_ok=True)
 
        logging.info(
            f"[{i}] upload_final_parquet: {perf_counter() - t:.2f}s | "
            f"s3://{bucket}/{target_key}"
        )
 
        logging.info(
            f"[{i}] total_archivo: {perf_counter() - t_file:.2f}s"
        )
 
    logging.info(
        f"Tiempo total transform_ipm_1240: {perf_counter() - t_total:.2f}s"
    )

# ==============================================================================
# MTI 1442
# ==============================================================================
 
def transform_ipm_1442(
        client_id: str, 
        file_id: str,
        file_type: str,
        context=None,
        content_hash: str = "",
) -> None:
 
    t_total = perf_counter()
    db = Database()
    
    file_config = fs._get_file_details(
        client_id=client_id,
        file_id=file_id,
    )
 
    #dict_de = db.get_de_layout("1240")
    dict_de, dict_pds = db.get_layout_by_mti("1442")
    _, container_cols = get_base_cols_and_containers("1442")
 
    file_processing_date = file_config["file_processing_date"]
    #file_type = str(file_config["file_type"]).strip().upper()
    file_type = file_type
    # 1) Obtener lista de parquets derivados
    keys = fs.list_parquet_files(
        layer=fs.Layer.STAGING,
        client_id=client_id,
        file_id=file_id,
        file_type=file_type,
        subdir="100_IPM_1442_RAW",
    )
 
    logging.info(f"Total parquets encontrados: {len(keys)}")
    
    #2) Iterar la lista para leer los parquets
    for i, key in enumerate(keys, start=1):
        
        t_file = perf_counter()
        
        logging.info(f"[{i}/{len(keys)}] Procesando key: {key}")
 
        filename = (Path(key).name)
 
        if context:
            logging.info(
                f"[{i}] Tiempo restante Lambda: "
                f"{context.get_remaining_time_in_millis() / 1000:.2f}s"
            )
            
        t = perf_counter()
          
        df = fs.read_parquet_by_key(
            layer=fs.Layer.STAGING,
            key=key,
        )
 
        logging.info(f"[{i}] read_parquet: {perf_counter() - t:.2f}s | rows={len(df)} | cols={len(df.columns)}")
 
        t = perf_counter()
        df_de_only = filter_df_columns_de(df=df, mti = '1442', dict_de=dict_de )
        del df
 
        logging.info(f"[{i}] filter_df_columns_de: {perf_counter() - t:.2f}s | rows={len(df_de_only)} | cols={len(df_de_only.columns)}")

        # 4) Expandir los DE por subfields según el layout del mensaje
        t = perf_counter()
        df_expand = expand_subfields(df=df_de_only, mti='1442', dict_de=dict_de)
        del df_de_only
 
        logging.info(f"[{i}] expand_subfields: {perf_counter() - t:.2f}s | rows={len(df_expand)} | cols={len(df_expand.columns)}")
 
        t = perf_counter()
        df_expand = reorder_with_subfield(df=df_expand, mti='1442', dict_de=dict_de)
 
        logging.info(f"[{i}] expand_reorder_with_subields: {perf_counter() - t:.2f}s | rows={len(df_expand)} | cols={len(df_expand.columns)}")
 
        # 5) Logica los PDS y los PDS subfields
        t = perf_counter()
        df_expand = apply_pds_for_mti(df=df_expand, mti = '1442', dict_pds=dict_pds, container_cols=container_cols)
 
        logging.info(f"[{i}] expand_apply_pds_for_mti: {perf_counter() - t:.2f}s | rows={len(df_expand)} | cols={len(df_expand.columns)}")
            
        # 5.1) Rename
        t = perf_counter()
        df_expand = df_expand.rename(columns={"MSG_NO": "ref_id", "MTI": "type_mti",},)
 
        logging.info(f"[{i}] expand_rename_columns: {perf_counter() - t:.2f}s | rows={len(df_expand)} | cols={len(df_expand.columns)}")

        t = perf_counter()
        df_expand = df_expand.assign(
            file_processing_date=file_processing_date,
            file_id=file_id,
            content_hash=content_hash,
        )
 
        logging.info(f"[{i}] expand_assign: {perf_counter() - t:.2f}s | rows={len(df_expand)} | cols={len(df_expand.columns)}")
 
        # 6) Generar parquets
        t = perf_counter()
        fs.write_parquet(df=df_expand, layer= layer.STAGING , client_id=client_id, file_id=file_id, file_type = file_type, subdir= '200_IPM_1442_TRA', filename=filename)
        logging.info(f"[{i}] write_parquet: {perf_counter() - t:.2f}s")
        
        del df_expand
 
    logging.info(f"Tiempo total transform_ipm_1442: {perf_counter() - t_total:.2f}s")


# ==============================================================================
# MTI 1644
# ==============================================================================

def transform_ipm_1644(
    client_id: str, 
    file_id: str,
    file_type: str,
    context=None,
    content_hash: str = "",
) -> None:
 
    t_total = perf_counter()
    
    db = Database()
    dict_de, dict_pds = db.get_layout_by_mti("1644")
    
    file_config = fs._get_file_details(
        client_id=client_id,
        file_id=file_id,
    )
    file_processing_date = file_config["file_processing_date"]

    # 1) Obtener lista de parquets derivados
    keys = fs.list_parquet_files(
        layer=fs.Layer.STAGING,
        client_id=client_id,
        file_id=file_id,
        file_type=file_type,
        subdir="100_IPM_1644_RAW",
    )
 
    logging.info(f"Total parquets encontrados: {len(keys)}")
 
    if not keys:
        raise ValueError("No se encontraron parquets para procesar.")
    
    #2) Iterar la lista para leer los parquets
    for i, key in enumerate(keys, start=1):
 
        t_file = perf_counter()
        
        logging.info(f"[{i}/{len(keys)}] Procesando key: {key}")
 
        filename = (Path(key).name)
        
        t = perf_counter()
        df = fs.read_parquet_by_key(
            layer=fs.Layer.STAGING,
            key=key,
        )
 
        logging.info(f"[{i}] read_parquet: {perf_counter() - t:.2f}s | rows={len(df)} | cols={len(df.columns)}")
 
        t = perf_counter()
        df_de_only = filter_df_columns_de(df=df, mti = '1644', dict_de=dict_de)
        del df
        logging.info(f"[{i}] filter_df_columns_de: {perf_counter() - t:.2f}s | rows={len(df_de_only)} | cols={len(df_de_only.columns)}")
 
        t = perf_counter()
        dfs = apply_pds_for_mti_1644_split(df_de_only, dict_pds_1644=dict_pds)
        del df_de_only
        #logging.info(f"[{i}] apply_pds_for_mti_1644_split: {perf_counter() - t:.2f}s | rows={len(dfs)} | cols={len(dfs.columns)}")
 
        
        df_685 = dfs.get("685")
        df_688 = dfs.get("688")
        df_691= dfs.get("691")
        

        # 6) Generar parquets
        t = perf_counter()
        if df_685 is not None and not df_685.empty:
            df_685["content_hash"]         = content_hash
            df_685["file_id"]              = file_id
            df_685["file_processing_date"] = file_processing_date
            fs.write_parquet(df=df_685, layer= layer.STAGING , client_id=client_id, file_id=file_id, file_type=file_type, subdir= '200_IPM_1644_TRA', filename=f'{filename.replace(".parquet", "_685.parquet")}')
        if df_688 is not None and not df_688.empty:
            df_688["content_hash"]         = content_hash
            df_688["file_id"]              = file_id
            df_688["file_processing_date"] = file_processing_date
            fs.write_parquet(df=df_688, layer= layer.STAGING , client_id=client_id, file_id=file_id, file_type=file_type, subdir= '200_IPM_1644_TRA', filename=f'{filename.replace(".parquet", "_688.parquet")}')
        if df_691 is not None and not df_691.empty:
            df_691["content_hash"]         = content_hash
            df_691["file_id"]              = file_id
            df_691["file_processing_date"] = file_processing_date
            fs.write_parquet(df=df_691, layer= layer.STAGING , client_id=client_id, file_id=file_id, file_type=file_type,subdir= '200_IPM_1644_TRA', filename=f'{filename.replace(".parquet", "_691.parquet")}')
 
        del df_685, df_688, df_691, dfs

# ==============================================================================
# MTI 1740
# ==============================================================================

def transform_ipm_1740(
        client_id: str, 
        file_id: str, 
        file_type: str,
        context=None,
        content_hash: str = "",
) -> None:
    
    t_total = perf_counter()
    db = Database()
    
    file_config = fs._get_file_details(
        client_id=client_id,
        file_id=file_id,
    )
    file_processing_date = file_config["file_processing_date"]

    #dict_de = db.get_de_layout("1240")
    dict_de, dict_pds = db.get_layout_by_mti("1740")
    _, container_cols = get_base_cols_and_containers("1740")
 
    # 1) Obtener lista de parquets derivados
    keys = fs.list_parquet_files(
        layer=fs.Layer.STAGING,
        client_id=client_id,
        file_id=file_id,
        file_type=file_type,
        subdir="100_IPM_1740_RAW",
    )
    
    # 2) Iterar la lista para leer los parquets
    for i, key in enumerate(keys, start=1):
 
        t_file = perf_counter()
        
        logging.info(f"[{i}/{len(keys)}] Procesando key: {key}")
 
        filename = (Path(key).name)
 
        if context:
            logging.info(
                f"[{i}] Tiempo restante Lambda: "
                f"{context.get_remaining_time_in_millis() / 1000:.2f}s"
            )
            
        t = perf_counter()
          
        df = fs.read_parquet_by_key(
            layer=fs.Layer.STAGING,
            key=key,
        )
 
        logging.info(f"[{i}] read_parquet: {perf_counter() - t:.2f}s | rows={len(df)} | cols={len(df.columns)}")
 
        t = perf_counter()
        df_de_only = filter_df_columns_de(df=df, mti = '1740', dict_de=dict_de )
        del df
 
        logging.info(f"[{i}] filter_df_columns_de: {perf_counter() - t:.2f}s | rows={len(df_de_only)} | cols={len(df_de_only.columns)}")

        # 4) Expandir los DE por subfields según el layout del mensaje
        t = perf_counter()
        df_expand = expand_subfields(df=df_de_only, mti='1740', dict_de=dict_de)
        del df_de_only
 
        logging.info(f"[{i}] expand_subfields: {perf_counter() - t:.2f}s | rows={len(df_expand)} | cols={len(df_expand.columns)}")
 
        t = perf_counter()
        df_expand = reorder_with_subfield(df=df_expand, mti='1740', dict_de=dict_de)
 
        logging.info(f"[{i}] expand_reorder_with_subields: {perf_counter() - t:.2f}s | rows={len(df_expand)} | cols={len(df_expand.columns)}")
 
        # 5) Logica los PDS y los PDS subfields
        t = perf_counter()
        df_expand = apply_pds_for_mti(df=df_expand, mti = '1740', dict_pds=dict_pds, container_cols=container_cols)
 
        logging.info(f"[{i}] expand_apply_pds_for_mti: {perf_counter() - t:.2f}s | rows={len(df_expand)} | cols={len(df_expand.columns)}")
            
        # 5.1) Rename
        t = perf_counter()
        df_expand = df_expand.rename(columns={"MSG_NO": "ref_id", "MTI": "type_mti",},)
 
        logging.info(f"[{i}] expand_rename_columns: {perf_counter() - t:.2f}s | rows={len(df_expand)} | cols={len(df_expand.columns)}")

        # 6) Generar parquets
        t = perf_counter()
        df_expand["content_hash"]         = content_hash
        df_expand["file_id"]              = file_id
        df_expand["file_processing_date"] = file_processing_date
        fs.write_parquet(df=df_expand, layer= layer.STAGING , client_id=client_id, file_id=file_id, file_type=file_type, subdir= '200_IPM_1740_TRA', filename=filename)
        logging.info(f"[{i}] write_parquet: {perf_counter() - t:.2f}s")
        
        del df_expand
   
TRANSFORMS = {
    "1240": transform_ipm_1240,
    "1442": transform_ipm_1442,
    "1644": transform_ipm_1644,
    "1740": transform_ipm_1740,
}

def detect_available_mtis(
    client_id: str,
    file_id: str,
    file_type: str,
) -> list[str]:
 
    mtis = []
 
    for mti in ("1240", "1442", "1644", "1740"):
 
        keys = fs.list_parquet_files(
            layer=fs.Layer.STAGING,
            client_id=client_id,
            file_id=file_id,
            file_type=file_type,
            subdir=f"100_IPM_{mti}_RAW",
        )
 
        if keys:
            mtis.append(mti)
 
    return mtis

# ============================================================
# 9. Handler Lambda
# ============================================================
 
layer = FileStorage.Layer
fs = FileStorage()   
 
def lambda_handler(event, context):
 
    logging.info(f"EVENT={json.dumps(event)}")
 
    # ------------------------------------------------------------------
    # 1. Validación de variables de entorno requeridas
    # ------------------------------------------------------------------
    s3_staging = os.environ.get("S3_STAGING_BUCKET")
    if not s3_staging:
        raise ValueError(
            "Falta variable de entorno requerida: S3_STAGING_BUCKET"
        )

    # ------------------------------------------------------------------
    # 2. Extracción de campos del evento
    #    Todos los campos vienen al nivel raíz, tal como los produce
    #    mc_interpreter en el Step Function.
    # ------------------------------------------------------------------
    client_id = event.get("client_id")
    file_id = event.get("file_id")
    brand = event.get("brand")
    brand_id = event.get("brand_id")
    file_type = event.get("file_type")
    file_date = event.get("file_date")
    content_hash = event.get("content_hash")
    filename = event.get("filename")
 
    if not client_id or not file_id:
        raise ValueError(
            f"Faltan campos obligatorios en el evento: "
            f"client_id={client_id!r}, file_id={file_id!r}"
        )
    
    logging.info(
        f"Processing: client={client_id}, brand={brand}, "
        f"type={file_type}, date={file_date}, file_id={file_id}"
    )
    
    # ------------------------------------------------------------------
    # 3. Detección de MTIs disponibles
    #    Se aprovecha interpreter_result.outputs (si existe) para
    #    derivar los MTIs sin hacer llamadas extra a S3.
    #    Si no está presente, se cae al fallback de detect_available_mtis.
    # ------------------------------------------------------------------
    interpreter_result = event.get("interpreter_result", {})
    outputs = interpreter_result.get("outputs", [])
 
    if outputs:
 
        mtis_from_outputs = list({
            output["mti"]
            for output in outputs
            if output.get("mti") in TRANSFORMS
        })
 
        if mtis_from_outputs:
            logging.info(
                f"MTIs derivados de interpreter_result.outputs: {mtis_from_outputs}"
            )
            mtis = mtis_from_outputs
 
        else:
            logging.warning(
                "No se pudieron derivar MTIs de interpreter_result.outputs. "
                "Ejecutando fallback con detect_available_mtis."
        )
 
            mtis = detect_available_mtis(
                client_id=client_id,
                file_id=file_id,
                file_type=file_type,
            )
 
    else:
        logging.info(
            "interpreter_result.outputs vacío. Ejecutando detect_available_mtis."
        )
 
        mtis = detect_available_mtis(
            client_id=client_id,
            file_id=file_id,
            file_type=file_type,
        )
    
    logging.info(f"MTIs a procesar: {mtis}")
 
    if not mtis:
        raise ValueError(
            f"No se encontraron MTIs para procesar: "
            f"client_id={client_id}, file_id={file_id}"
        )
    
    # ------------------------------------------------------------------
    # 4. Ejecución de transforms por MTI
    # ------------------------------------------------------------------
    t_global = perf_counter()
 
    for mti in mtis:
 
        transform_fn = TRANSFORMS[mti]
 
        logging.info(f"START transform_ipm_{mti}")
        t = perf_counter()
 
        transform_fn(
            client_id=client_id,
            file_id=file_id,
            file_type = file_type,
            content_hash=content_hash,
        )
 
        logging.info(
            f"END transform_ipm_{mti} | "
            f"time={perf_counter() - t:.2f}s"
        )
 
    logging.info(f"=== Done: {len(mtis)} MTIs procesados | " 
                 f"Tiempo total= {perf_counter() - t_global:.2f}s ===" )
    
    # ------------------------------------------------------------------
    # 5. Recolección de outputs reales escritos en STAGING
    #    Para cada MTI procesado, se listan los parquets generados en el
    #    subdir de salida (200_IPM_<MTI>_TRA) y se construyen las URIs
    #    s3:// completas, alineando el contrato de salida con el que
    #    produce mc_interpreter (rutas S3 reales, no códigos MTI).
    # ------------------------------------------------------------------
    staging_bucket = fs._get_bucket_by_layer(layer.STAGING)
    uploaded_outputs: list[str] = []
 
    for mti in mtis:
        output_subdir = f"200_IPM_{mti}_TRA"
        keys = fs.list_parquet_files(
            layer=layer.STAGING,
            client_id=client_id,
            file_type=file_type,
            file_id=file_id,
            subdir=output_subdir,
        )
        for key in keys:
            uploaded_outputs.append(f"s3://{staging_bucket}/{key}")
 
    logging.info(
        f"Outputs recolectados: {len(uploaded_outputs)} parquets "
        f"en {len(mtis)} MTIs"
    )
 
    uploaded_outputs_json = _build_outputs_for_stepfunction(uploaded_outputs)

    # ------------------------------------------------------------------
    # 6. Response plano para Step Functions
    #    Estructura alineada con mc_interpreter:
    #    outputs = lista de rutas S3 completas de los parquets generados.
    #    total_outputs = cantidad real de parquets escritos.
    # ------------------------------------------------------------------
    return {
        "status":           "SUCCESS" if uploaded_outputs else "ERROR",
        "total_outputs":    len(uploaded_outputs),
        "total_records":    0,
        "outputs":          uploaded_outputs_json,
        "client_id":        client_id,
        "file_id":          file_id,
        "brand":            brand,
        "brand_id":         brand_id,
        "file_type":        file_type,
        "file_date":        file_date,
        "content_hash":     content_hash,
        "filename":         filename,
    }