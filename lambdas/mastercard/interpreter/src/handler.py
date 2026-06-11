from __future__ import annotations
 
# ──────────────────────────────────────────────────────────────────────────────
# mc_interpreter_handler.py
# Módulo único y autosuficiente del intérprete IPM Mastercard.
#
# Contiene en un solo archivo:
#   1. Database          – acceso DynamoDB
#   2. FileStorage       – I/O en S3 y staging temporal /tmp
#   3. Helpers ISO-8583  – decode, bitmap, MTI, Parameters
#   4. Lógica de negocio – parsing DE, build_wide_row, encoding, headers
#   5. Lectores IO       – unblock_1014, read_len_prefixed_messages*
#   6. Writers Parquet   – write_parquet_by_mti_block_streaming, finalize
#   7. Orquestador       – interpretate_msg
# ──────────────────────────────────────────────────────────────────────────────
 
import gc
import io
import json
import logging
import logging.handlers
import os
import re
import shutil
import struct
from collections import OrderedDict
from collections.abc import Set as AbstractSet
from decimal import Decimal
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Tuple, cast
 
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

# ══════════════════════════════════════════════════════════════════════════════
# LOGGER
# ══════════════════════════════════════════════════════════════════════════════
 
# En Lambda todo lo enviado a stdout/stderr se captura en CloudWatch Logs.
 
class Logger:
    """
    Provides a standardized logger object to print and store log messages.
 
    En Lambda: solo StreamHandler -> stdout -> CloudWatch Logs automáticamente.
    En local: StreamHandler -> FileHandler -> consola -> archivo de log en disco.
    """
 
    _LOG_LEVELS = OrderedDict(
        {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
    )
 
    _DEFAULT_FMT = (
        "%(asctime)s :: PID %(process)d :: TID %(thread)d :: "
        "%(module)s.%(funcName)s :: Line %(lineno)d :: "
        "%(levelname)s :: %(message)s"
    )
 
    def __init__(self, name: str) -> None:
 
        self.logger = logging.getLogger(name)
 
        if self.logger.handlers:
            return
 
        log_level = os.environ.get("ITX_LOG_LEVEL", "info").strip().lower()
        self.logger.setLevel(self._LOG_LEVELS.get(log_level, logging.INFO))
 
        formatter = logging.Formatter(self._DEFAULT_FMT)
 
        # StreamHandler siempre activo
        # En Lambda, stdout es capturado automáticamente por CloudWatch.
        # En local, imprime en consola.
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
 
 
log = Logger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE / DYNAMODB
# ══════════════════════════════════════════════════════════════════════════════
 
class Database:
    """
    Acceso DynamoDB para las tablas necesarias del interpreter Mastercard.
 
    Tablas usadas:
      - file_control
      - file_name_regex_param
      - client
 
    Nota:
    Se conserva la interfaz read_records(...) del mc_interpreter original para
    minimizar cambios sobre la lógica de negocio ya validada.
    """
 
    DEFAULT_FILE_CONTROL_TABLE = "itl-0004-itx-dev-dynamo-file_control-02"
    DEFAULT_FILE_PATTERN_TABLE = "itl-0004-itx-dev-dynamo-file_pattern-02"
    DEFAULT_CLIENT_TABLE = "itl-0004-itx-dev-dynamo-client-02"
 
    def __init__(self) -> None:
        self.region = os.environ.get("AWS_REGION", "eu-south-2")
        self._dynamodb = None
        self._cache: dict[tuple[str, tuple[tuple[str, str], ...]], pd.DataFrame] = {}
 
    def _get_resource(self):
        if self._dynamodb is None:
            self._dynamodb = boto3.resource("dynamodb", region_name=self.region)
        return self._dynamodb
 
    def _resolve_table_name(self, table_name: str) -> str:
        normalized = table_name.strip().lower()
        mapping = {
            "file_control": os.environ.get(
                "ITX_DDB_FILE_CONTROL_TABLE",
                os.environ.get("ITX_TABLE_FILE_CONTROL", self.DEFAULT_FILE_CONTROL_TABLE),
            ),
            "file_name_regex_param": os.environ.get(
                "ITX_DDB_FILE_NAME_REGEX_PARAM_TABLE",
                os.environ.get("ITX_TABLE_FILE_NAME_REGEX_PARAM", self.DEFAULT_FILE_PATTERN_TABLE),
            ),
            # "file_pattern": os.environ.get(
            #     "ITX_DDB_FILE_NAME_REGEX_PARAM_TABLE",
            #     os.environ.get("ITX_TABLE_FILE_NAME_REGEX_PARAM", self.DEFAULT_FILE_PATTERN_TABLE),
            # ),
            "client": os.environ.get(
                "ITX_DDB_CLIENT_TABLE",
                os.environ.get("ITX_TABLE_CLIENT", self.DEFAULT_CLIENT_TABLE),
            ),
        }
        return mapping.get(normalized, table_name)
 
    def _get_table(self, logical_table_name: str):
        return self._get_resource().Table(self._resolve_table_name(logical_table_name))
 
    @staticmethod
    def _to_str(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, Decimal):
            return str(value)
        return str(value)
 
    @staticmethod
    def _match_where(item: dict[str, Any], where: dict[str, str | int | float]) -> bool:
        for key, expected in where.items():
            actual = item.get(key)
            if actual is None:
                return False
            if str(actual).strip().upper() != str(expected).strip().upper():
                return False
        return True
 
    def _scan_all(self, logical_table_name: str) -> list[dict[str, Any]]:
        table = self._get_table(logical_table_name)
        items: list[dict[str, Any]] = []
        scan_kwargs: dict[str, Any] = {}
 
        while True:
            response = table.scan(**scan_kwargs)
            items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key
 
        return items
 
    def _try_get_item(self, logical_table_name: str, where: dict[str, str | int | float]) -> dict[str, Any] | None:
        """
        Intenta get_item para tablas con keys conocidas. Si no aplica o falla por
        esquema de key distinto, retorna None y el caller hace fallback a scan.
        """
        table = self._get_table(logical_table_name)
        normalized = logical_table_name.strip().lower()
 
        candidate_keys: list[dict[str, str | int | float]] = []
        if normalized == "file_control" and "file_id" in where:
            candidate_keys.append({"file_id": where["file_id"]})
            if "client_id" in where:
                candidate_keys.append({"client_id": where["client_id"], "file_id": where["file_id"]})
        elif normalized == "client" and "client_id" in where:
            candidate_keys.append({"client_id": where["client_id"]})
 
        for key in candidate_keys:
            try:
                response = table.get_item(Key=key)
            except ClientError as exc:
                # ValidationException suele indicar que la key enviada no coincide
                # con el esquema de la tabla. En ese caso hacemos fallback a scan.
                code = exc.response.get("Error", {}).get("Code", "")
                if code == "ValidationException":
                    continue
                raise
 
            item = response.get("Item")
            if item and self._match_where(item, where):
                return item
 
        return None
 
    def _to_bool(self, val: object) -> bool:
        if val is None:
            return False
        if isinstance(val, bool):
            return val
        return str(val).strip().lower() in ("true", "1", "y", "yes", "t")

    def read_records(
        self,
        table_name: str,
        fields: list[str],
        where: dict[str, str | int | float] | None = None,
    ) -> pd.DataFrame:
        """
        Lee registros desde DynamoDB y devuelve un DataFrame con las columnas
        solicitadas. Mantiene compatibilidad con la firma original de SQLite.
        """
        if where is None:
            where = {}
 
        # cache_key includes fields so that two calls to the same table+where
        # but requesting different columns don't share the same cache entry
        # (which would cause reindex to return NaN for missing columns).
        cache_key = (
            table_name.strip().lower(),
            tuple(sorted(fields)),
            tuple(sorted((str(k), str(v)) for k, v in where.items())),
        )
        if cache_key in self._cache:
            return self._cache[cache_key].copy()
 
        log.logger.debug(
            f"DynamoDB read_records | table={table_name} | fields={fields} | where={where}"
        )
 
        items: list[dict[str, Any]] = []
 
        if where:
            item = self._try_get_item(table_name, where)
            if item is not None:
                items = [item]
            else:
                scanned = self._scan_all(table_name)
                items = [it for it in scanned if self._match_where(it, where)]
        else:
            items = self._scan_all(table_name)
 
        rows = [
            {field: self._to_str(item.get(field, "")) for field in fields}
            for item in items
        ]
        df = pd.DataFrame(rows, columns=fields, dtype=str)
        self._cache[cache_key] = df.copy()
        return df
 
    def read_sql(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """
        Compatibilidad mínima para las consultas usadas por obtain_encoding(...).
        No es un motor SQL; traduce las dos consultas existentes a read_records.
        """
        sql_lower = " ".join(sql.lower().split())
 
        if "from file_control" in sql_lower and "file_type" in sql_lower:
            client_id, file_id = params
            return self.read_records(
                table_name="file_control",
                fields=["file_type"],
                where={"client_id": client_id, "file_id": file_id},
            )
 
        if "from client" in sql_lower:
            client_id = params[0]
            if "file_mc_encoding_in" in sql_lower:
                col = "file_mc_encoding_in"
            elif "file_mc_encoding_out" in sql_lower:
                col = "file_mc_encoding_out"
            else:
                raise ValueError(f"Consulta read_sql no soportada: {sql}")
            return self.read_records(
                table_name="client",
                fields=[col],
                where={"client_id": client_id},
            )
 
        raise ValueError(f"Consulta read_sql no soportada en Lambda handler: {sql}")

    def needs_unblock_for_file(self, client_id: str, file_id: str) -> bool:
        df_cf = self.read_records(
            table_name="file_control",
            fields=["file_type", "brand_id", "landing_file_name"],
            where={"client_id": client_id, "file_id": file_id},
        )
 
        if df_cf.empty:
            log.logger.info(f"[needs_unblock] df_cf vacío para client_id={client_id}, file_id={file_id}")
            return False
 
        file_type = str(df_cf.iloc[0]["file_type"] or "").strip().upper()
        raw_brand_id = str(df_cf.iloc[0]["brand_id"] or "").strip().upper()
        landing_file_name = str(df_cf.iloc[0]["landing_file_name"] or "").strip()
        
        if raw_brand_id in ("VI", "VISA"):
            brand_id = "VISA"
        elif raw_brand_id in ("MC", "MASTERCARD"):
            brand_id = "MASTERCARD"
        else:
            brand_id = "UNKNOWN"
            log.logger.warning(
                f"brand_id desconocido en file_control: "
                f"client_id={client_id}, file_id={file_id}, brand_id={raw_brand_id}"
            )
 
        log.logger.info(
            f"[needs_unblock] file_type={file_type!r} | brand_id={brand_id!r} | "
            f"landing_file_name={landing_file_name!r}"
        )
 
        df_rx = self.read_records(
            table_name="file_name_regex_param",
            fields=["file_format", "file_block"],
            where={"customer_code": client_id, "brand": brand_id, }, # REMOVED: "file_type": file_type
        )
 
        log.logger.info(
            f"[needs_unblock] df_rx rows={len(df_rx)} | "
            f"query where: customer_code={client_id!r}, brand={brand_id!r}, file_type={file_type!r}"
        )
 
        if not df_rx.empty:
            log.logger.info(f"[needs_unblock] df_rx sample:\n{df_rx.to_string()}")
 
        if df_rx.empty:
            return False
 
        for _, row in df_rx.iterrows():
            pattern = str(row["file_format"]).strip()
            matched = False
            try:
                # if re.match(pattern=pattern, string=landing_file_name, flags=re.IGNORECASE):
                #     return self._to_bool(row["file_block"])
                matched = bool(re.match(pattern=pattern, string=landing_file_name, flags=re.IGNORECASE))
            except re.error:
                log.logger.warning(f"Regex inválida en file_name_regex_param: {pattern}")
                log.logger.warning(f"[needs_unblock] Regex inválida: {pattern}")
                continue
 
            log.logger.info(
                f"[needs_unblock] pattern={pattern!r} | "
                f"landing={landing_file_name!r} | matched={matched} | "
                f"file_block raw={row['file_block']!r}"
            )
 
            if matched:
                return self._to_bool(row["file_block"])
 
        return False

    def needs_interpreter_fix(self, client_id: str, file_id: str) -> bool:
        log.logger.info(
            f"[Search need_interpreter_fix value] client_id: {client_id} | file_id: {file_id}"
        )
        df_cf = self.read_records(
            table_name="file_control",
            fields=["file_type", "brand_id", "landing_file_name"],
            where={"client_id": client_id, "file_id": file_id},
        )
 
        log.logger.info(
            f"[Search need_interpreter_fix value] client_id: {client_id} | file_id: {file_id} | len df: {len(df_cf)}"
        )
 
        if df_cf.empty:
            return False
 
        file_type = str(df_cf.iloc[0]["file_type"] or "").strip().upper()
        raw_brand_id = str(df_cf.iloc[0]["brand_id"] or "").strip().upper()
 
        log.logger.info(
            f"[Search need_interpreter_fix value] client_id: {client_id} | file_id: {file_id} | len df: {len(df_cf)} | file_type = {file_type} | raw_brand_id : {raw_brand_id}"
        )
        
        if raw_brand_id in ("VI", "VISA"):
            brand_id = "VISA"
        elif raw_brand_id in ("MC", "MASTERCARD"):
            brand_id = "MASTERCARD"
        else:
            brand_id = "UNKNOWN"
            log.logger.warning(
                f"brand_id desconocido en file_control: "
                f"client_id={client_id}, file_id={file_id}, brand_id={raw_brand_id}"
            )
 
        log.logger.info(
            f"[Search need_interpreter_fix value] client_id: {client_id} | file_id: {file_id} | len df: {len(df_cf)} | file_type = {file_type} | brand_id : {brand_id}"
        )
        
        landing_file_name = str(df_cf.iloc[0]["landing_file_name"] or "").strip()
 
        log.logger.info(
            f"[Search need_interpreter_fix value] client_id: {client_id} | file_id: {file_id} | len df: {len(df_cf)} | file_type = {file_type} | brand_id : {brand_id} | landing_file_name: {landing_file_name}"
        )
 
        df_rx = self.read_records(
            table_name="file_name_regex_param",
            fields=["file_format", "interpreter_fix"],
            where={"customer_code": client_id, "brand": brand_id},  # REMOVED: "file_type": file_type
        )
 
        log.logger.info(
            f"[Search need_interpreter_fix value] client_id: {client_id} | " 
            f"file_id: {file_id} | len df: {len(df_cf)} | file_type = {file_type} | " 
            f"brand_id : {brand_id} | landing_file_name: {landing_file_name} | "
            f" len df_rx: {len(df_rx)}"
        )
 
        if df_rx.empty:
            return False
 
        for _, row in df_rx.iterrows():
            pattern = str(row["file_format"]).strip()
            try:
                if re.match(pattern=pattern, string=landing_file_name, flags=re.IGNORECASE):
                    return self._to_bool(row["interpreter_fix"])
            except re.error:
                log.logger.warning(f"Regex inválida en file_name_regex_param: {pattern}")
                continue
        return False

# ══════════════════════════════════════════════════════════════════════════════
# FILE STORAGE / S3
# ══════════════════════════════════════════════════════════════════════════════
 
class _Layer(StrEnum):
    """Enum de capas de almacenamiento."""
    LANDING = auto()
    STAGING = auto()
    OPERATIONAL = auto()
 
 
class FileStorage:
    """
    Capa de I/O sobre S3 para Lambda.
 
    LANDING:
        s3://{ITX_S3_BUCKET_LANDING}/{client_id}/{landing_file_name}
 
    STAGING (nueva estructura con particionado Hive-style):
        s3://{ITX_S3_BUCKET_STAGING}/{client_id}/{brand_id}/{subdir}/file_type={file_type}/date={file_processing_date}/{filename}
 
        Ejemplo:
        s3://.../EURBGR/MC/100_IPM_1240_RAW/file_type=IN/date=20240101/abc123_IDN001_1240.parquet
 
    /tmp local (misma jerarquía, relativa a ITX_TMP_ROOT):
        {tmp_root}/{client_id}/{brand_id}/{subdir}/file_type={file_type}/date={file_processing_date}/{filename}
 
    Para mantener la lógica original con pyarrow.ParquetWriter, los parquets se
    escriben primero en /tmp y luego se suben a S3 al finalizar el interpreter.
    """
 
    Layer = _Layer
 
    DEFAULT_BUCKET_LANDING = "itl-0004-itx-dev-intchg-02-s3-landing"
    DEFAULT_BUCKET_STAGING = "itl-0004-itx-dev-intchg-02-s3-staging"
 
    def __init__(self) -> None:
        self.region = os.environ.get("AWS_REGION", "eu-south-2")
        self.tmp_root = Path(os.environ.get("ITX_TMP_ROOT", "/tmp/mc_interpreter"))
        self._s3 = None
        self._details_cache: dict[tuple[str, str], dict[str, Any]] = {}
 
    def _get_client(self):
        if self._s3 is None:
            self._s3 = boto3.client("s3", region_name=self.region)
        return self._s3
 
    def _get_bucket(self, layer: _Layer) -> str:
        mapping = {
            self.Layer.LANDING: (
                "ITX_S3_BUCKET_LANDING",
                self.DEFAULT_BUCKET_LANDING,
            ),
            self.Layer.STAGING: (
                "ITX_S3_BUCKET_STAGING",
                self.DEFAULT_BUCKET_STAGING,
            ),
            self.Layer.OPERATIONAL: (
                "ITX_S3_BUCKET_OPERATIONAL",
                "",
            ),
        }
        env_var, default = mapping[layer]
        bucket = os.environ.get(env_var, default)
        if not bucket:
            raise ValueError(f"No hay bucket configurado para layer={layer}")
        return bucket
 
    def get_file_control_details(
        self,
        client_id: str,
        file_id: str,
        fields: list[str] | None = None,
    ) -> dict[str, Any]:
        if fields is None:
            fields = [
                "client_id",
                "brand_id",
                "file_type",
                "file_processing_date",
                "landing_file_name",
                "file_id",
            ]
 
        cache_key = (client_id.strip().upper(), file_id.strip().upper())
        if cache_key not in self._details_cache:
            db = Database()
            df = db.read_records(
                table_name="file_control",
                fields=list(set(fields + ["client_id", "file_id"])),
                where={"client_id": client_id, "file_id": file_id},
            )
            if df.empty:
                raise ValueError(
                    f"No existe file_control para client_id={client_id}, file_id={file_id}"
                )
            self._details_cache[cache_key] = df.iloc[0].to_dict()
 
        details = self._details_cache[cache_key]
        return {field: details.get(field, "") for field in fields}
 
    def _get_s3_key_prefix(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
    ) -> str:
        details = self.get_file_control_details(client_id=client_id, file_id=file_id)
 
        if layer == self.Layer.LANDING:
            return f"{client_id}/"
 
        # Nueva estructura: client/brand/[subdir/]file_type=X/date=DATE/
        parts = [
            str(details["client_id"] or client_id),
            str(details["brand_id"]),
        ]
        if subdir:
            parts.append(subdir.strip("/"))
        parts.append(f"file_type={details['file_type']}")
        parts.append(f"date={details['file_processing_date']}")
        return "/".join(parts) + "/"
 
    def _get_s3_key(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
    ) -> str:
        details = self.get_file_control_details(client_id=client_id, file_id=file_id)
        prefix = self._get_s3_key_prefix(layer, client_id, file_id, subdir)
 
        if layer == self.Layer.LANDING:
            return prefix + str(details["landing_file_name"])
 
        return prefix + file_id
 
    def _get_local_root(self, client_id: str, file_id: str) -> Path:
        details = self.get_file_control_details(client_id=client_id, file_id=file_id)
        # Raiz base: solo client/brand
        # Los segmentos subdir, file_type= y date= se añaden en _get_file_path/_get_folder_path.
        return self.tmp_root / str(details["client_id"] or client_id) / str(details["brand_id"])
 
    def _get_file_path(
        self, layer: _Layer, client_id: str, file_id: str, subdir: str = ""
    ) -> str:
        """
        Retorna ruta local temporal para writers parquet.
        LANDING no usa esta ruta para lectura; LANDING se lee directo de S3.
 
        STAGING: tmp_root/client/brand/[subdir/]file_type=X/date=DATE/file_id
        """
        if layer == self.Layer.LANDING:
            return str(self.tmp_root / "landing" / client_id / file_id)
 
        details = self.get_file_control_details(client_id=client_id, file_id=file_id)
        file_type = str(details["file_type"])
        date      = str(details["file_processing_date"])
        base      = self._get_local_root(client_id=client_id, file_id=file_id)
        if subdir:
            base = base / subdir.strip("/")
        return str(base / f"file_type={file_type}" / f"date={date}" / file_id)
 
    def _get_folder_path(
        self, layer: _Layer, client_id: str, file_id: str, subdir: str = ""
    ) -> str:
        if layer == self.Layer.LANDING:
            return str(self.tmp_root / "landing" / client_id)
 
        details = self.get_file_control_details(client_id=client_id, file_id=file_id)
        file_type = str(details["file_type"])
        date      = str(details["file_processing_date"])
        base      = self._get_local_root(client_id=client_id, file_id=file_id)
        if subdir:
            base = base / subdir.strip("/")
        return str(base / f"file_type={file_type}" / f"date={date}")
 
    def cleanup_tmp_outputs(self, client_id: str, file_id: str) -> None:
        local_root = self._get_local_root(client_id=client_id, file_id=file_id)
        if local_root.exists():
            shutil.rmtree(local_root)
 
    def read_binary(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        in_memory: bool = True,
    ) -> BinaryIO:
        bucket = self._get_bucket(layer)
        key = self._get_s3_key(layer, client_id, file_id, subdir)
 
        log.logger.info(f"Leyendo binario desde S3: s3://{bucket}/{key}")
        try:
            response = self._get_client().get_object(Bucket=bucket, Key=key)
            data = response["Body"].read()
            return io.BytesIO(data)
        except ClientError as exc:
            log.logger.error(
                f"Error S3 get_object [{exc.response['Error']['Code']}] | s3://{bucket}/{key}"
            )
            raise
 
    def read_parquet(
        self, layer: _Layer, client_id: str, file_id: str, subdir: str = ""
    ) -> pd.DataFrame:
        bucket = self._get_bucket(layer)
        key = self._get_s3_key(layer, client_id, file_id, subdir) + ".parquet"
        log.logger.info(f"Leyendo parquet desde S3: s3://{bucket}/{key}")
        response = self._get_client().get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(response["Body"].read()))
 
    def write_parquet(
        self,
        data: pd.DataFrame,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        index: bool = False,
    ) -> str:
        bucket = self._get_bucket(layer)
        key = self._get_s3_key(layer, client_id, file_id, subdir) + ".parquet"
        buffer = io.BytesIO()
        data.to_parquet(buffer, index=index)
        buffer.seek(0)
        self._get_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        return f"s3://{bucket}/{key}"
 
    def upload_tmp_outputs(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
    ) -> list[str]:
        """
        Sube a S3 todos los parquets generados en /tmp para este archivo.
 
        local_root  = tmp_root/client/brand
        root_prefix = client/brand/          (prefijo base en S3)
 
        Los archivos están en:
            local_root/subdir/file_type=X/date=DATE/filename.parquet
 
        relative_key =  subdir/file_type=X/date=DATE/filename.parquet
        s3_key       =  client/brand/subdir/file_type=X/date=DATE/filename.parquet
        """
        if layer != self.Layer.STAGING:
            raise ValueError("upload_tmp_outputs solo está implementado para STAGING")
 
        local_root = self._get_local_root(client_id=client_id, file_id=file_id)
        bucket     = self._get_bucket(layer)
 
        # El prefijo base en S3 es solo client/brand/ para que los relative_key
        # que ya incluyen subdir/file_type=X/date=DATE/ se peguen correctamente.
        details     = self.get_file_control_details(client_id=client_id, file_id=file_id)
        root_prefix = (
            f"{str(details['client_id'] or client_id)}"
            f"/{str(details['brand_id'])}/"
        )
 
        if not local_root.exists():
            log.logger.warning(f"No hay outputs temporales para subir: {local_root}")
            return []
 
        uploaded: list[str] = []
        for parquet_path in sorted(local_root.rglob("*.parquet")):
            if not parquet_path.name.startswith(file_id):
                continue
            relative_key = parquet_path.relative_to(local_root).as_posix()
            s3_key = root_prefix + relative_key
            log.logger.info(f"Subiendo parquet a S3: {parquet_path} -> s3://{bucket}/{s3_key}")
            self._get_client().upload_file(
                Filename=str(parquet_path),
                Bucket=bucket,
                Key=s3_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )
            uploaded.append(f"s3://{bucket}/{s3_key}")
 
        shutil.rmtree(local_root, ignore_errors=True)
        return uploaded

# ══════════════════════════════════════════════════════════════════════════════
# 3. ISO-8583 / PARSING HELPERS
# ══════════════════════════════════════════════════════════════════════════════
 
def decode_digits(b: bytes, enc: str) -> str:
    """
    Convierte bytes que representan dígitos (ASCII o EBCDIC) a string.
    - ASCII:  b'697'  -> '697'
    - EBCDIC: 0xF0..0xF9 -> '0'..'9'
    """
    enc = (enc or "").lower()
    if "ebcdic" in enc:
        out = []
        for x in b:
            if 0xF0 <= x <= 0xF9:
                out.append(chr(ord("0") + (x - 0xF0)))
            else:
                out.append("?")
        return "".join(out)
    return b.decode("latin1", errors="ignore")
 
 
def bitmap_bits(bitmap: bytes) -> list[int]:
    """
    Devuelve la lista de campos presentes según el bitmap.
    - 8 bytes  -> campos 1..64
    - 16 bytes -> campos 1..128
    Convención: bit más significativo (MSB) primero.
    """
    fields: list[int] = []
    for i, byte in enumerate(bitmap):
        for bit in range(8):
            if byte & (1 << (7 - bit)):
                fields.append(i * 8 + bit + 1)
    return fields


def split_mti_bitmap_body(payload: bytes):
    """
    Separa payload en mti_bytes (4), bitmap_bytes (8 o 16) y body_bytes (resto).
    Asume payload = MTI (4 bytes) + bitmap primario (8 bytes).
    Si el bit 1 del bitmap primario está activo, hay bitmap secundario (8 bytes).
    """
    if len(payload) < 12:
        return None
 
    mti_bytes = payload[:4]
    primary   = payload[4:12]
 
    has_secondary = bool(primary[0] & 0x80)
 
    if has_secondary:
        if len(payload) < 20:
            return None
        secondary = payload[12:20]
        bitmap    = primary + secondary
        body      = payload[20:]
    else:
        bitmap = primary
        body   = payload[12:]
 
    fields = bitmap_bits(bitmap)
    return mti_bytes, bitmap, body, fields, has_secondary
 
 
def detect_mti(payload: bytes, encoding: str):
    """
    Intenta detectar si un mensaje empieza con un MTI válido (ISO-8583).
    Devuelve: (mti_str, 'ASCII' o 'EBCDIC_DIGITS') o (None, None).
    """
    if len(payload) < 4:
        return None, None
 
    m4 = payload[:4]
    if encoding.upper() in ("LATIN-1", "LATIN1", "ISO-8859-1", "ASCII"):
        return m4.decode("ascii"), "ASCII"
    elif encoding.upper() in ("CP500", "EBCDIC", "EBCDIC_DIGITS"):
        return "".join(str(b - 0xF0) for b in m4), "EBCDIC_DIGITS"
    else:
        return None, None
    

class Parameters:
    """Class for storing parameters for Mastercard files."""
 
    def __init__(self, *args):
        super(Parameters, self).__init__(*args)
 
    def getdataelements(self) -> dict:
        """Store data elements configuration."""
        dataelements = {
            1:   {"fixed": True,  "length": 8},
            2:   {"fixed": False, "length": 2},
            3:   {"fixed": True,  "length": 6},
            4:   {"fixed": True,  "length": 12},
            5:   {"fixed": True,  "length": 12},
            6:   {"fixed": True,  "length": 12},
            9:   {"fixed": True,  "length": 8},
            10:  {"fixed": True,  "length": 8},
            12:  {"fixed": True,  "length": 12},
            14:  {"fixed": True,  "length": 4},
            22:  {"fixed": True,  "length": 12},
            23:  {"fixed": True,  "length": 3},
            24:  {"fixed": True,  "length": 3},
            25:  {"fixed": True,  "length": 4},
            26:  {"fixed": True,  "length": 4},
            30:  {"fixed": True,  "length": 24},
            31:  {"fixed": False, "length": 2},
            32:  {"fixed": False, "length": 2},
            33:  {"fixed": False, "length": 2},
            37:  {"fixed": True,  "length": 12},
            38:  {"fixed": True,  "length": 6},
            40:  {"fixed": True,  "length": 3},
            41:  {"fixed": True,  "length": 8},
            42:  {"fixed": True,  "length": 15},
            43:  {"fixed": False, "length": 2},
            48:  {"fixed": False, "length": 3},
            49:  {"fixed": True,  "length": 3},
            50:  {"fixed": True,  "length": 3},
            51:  {"fixed": True,  "length": 3},
            54:  {"fixed": False, "length": 3},
            55:  {"fixed": False, "length": 3},
            62:  {"fixed": False, "length": 3},
            63:  {"fixed": False, "length": 3},
            71:  {"fixed": True,  "length": 8},
            72:  {"fixed": False, "length": 3},
            73:  {"fixed": True,  "length": 6},
            93:  {"fixed": False, "length": 2},
            94:  {"fixed": False, "length": 2},
            95:  {"fixed": False, "length": 2},
            100: {"fixed": False, "length": 2},
            105: {"fixed": False, "length": 3},
            111: {"fixed": False, "length": 3},
            123: {"fixed": False, "length": 3},
            124: {"fixed": False, "length": 3},
            125: {"fixed": False, "length": 3},
            127: {"fixed": False, "length": 3},
        }
        return dataelements
    
    def getIPMParameters(self) -> dict:
        """Store IPM tables parameters."""
        params = {
            "update_header": {
                "header": {
                    "header_title": {"start": 0,  "end": 15},
                    "header_date":  {"start": 15, "end": 23},
                    "header_time":  {"start": 23, "end": 28},
                }
            },
            "replace_header": {
                "header": {
                    "header_title": {"start": 0,  "end": 17},
                    "header_date":  {"start": 45, "end": 54},
                    "header_time":  {"start": 61, "end": 69},
                }
            },
            "key": {
                "layout":       "IP0000T1",
                "key":          {"start": 11,  "end": 19},
                "table_ipm_id": {"start": 19,  "end": 27},
                "table_sub_id": {"start": 243, "end": 246},
            },
            "record": {"start": 8, "end": 11},
            "tables": {
                "IP0040T1": {
                    "effective_timestamp":                         {"start": 0,   "end": 7},
                    "active_inactive_code":                        {"start": 7,   "end": 8},
                    "table_id":                                    {"start": 8,   "end": 11},
                    "low_range":                                   {"start": 11,  "end": 30,  "data_type": "int64"},
                    "gcms_product":                                {"start": 30,  "end": 33},
                    "high_range":                                  {"start": 33,  "end": 52,  "data_type": "int64"},
                    "card_program_identifier":                     {"start": 52,  "end": 55},
                    "card_program_priority":                       {"start": 55,  "end": 57},
                    "member_id":                                   {"start": 57,  "end": 68},
                    "product_type":                                {"start": 68,  "end": 69},
                    "endpoint":                                    {"start": 69,  "end": 76},
                    "card_country_alpha":                          {"start": 76,  "end": 79},
                    "card_country_numeric":                        {"start": 79,  "end": 82},
                    "region":                                      {"start": 82,  "end": 83},
                    "product_class":                               {"start": 83,  "end": 86},
                    "tran_routing_ind":                            {"start": 86,  "end": 87},
                    "first_present_reassign_ind":                  {"start": 87,  "end": 88},
                    "product_reassign_switch":                     {"start": 88,  "end": 89},
                    "pwcb_optin_switch":                           {"start": 89,  "end": 90},
                    "licensed_product_id":                         {"start": 90,  "end": 93},
                    "mapping_service_ind":                         {"start": 93,  "end": 94},
                    "alm_participation_ind":                       {"start": 94,  "end": 95},
                    "alm_activation_date":                         {"start": 95,  "end": 101},
                    "cardholder_billing_currency_default":         {"start": 101, "end": 104},
                    "cardholder_billing_currency_exponent_default":{"start": 104, "end": 105},
                    "cardholder_billing_primary_currency":         {"start": 105, "end": 133},
                    "chip_to_magnetic":                            {"start": 133, "end": 134},
                    "floor_expiration_date":                       {"start": 134, "end": 140},
                    "co_brand_participation_switch":               {"start": 140, "end": 141},
                    "spend_control_switch":                        {"start": 141, "end": 142},
                    "merchant_cleansing_service":                  {"start": 142, "end": 145},
                    "merchant_cleansing_activation":               {"start": 145, "end": 151},
                    "contactless_enabled_indicator":               {"start": 151, "end": 152},
                    "regulated_rate_type":                         {"start": 152, "end": 153},
                    "psn_route_indicator":                         {"start": 153, "end": 154},
                    "cashback_without_purchase_indicator":         {"start": 154, "end": 155},
                    "repower_reload_participation_indicator":      {"start": 156, "end": 157},
                    "moneysend_indicator":                         {"start": 157, "end": 158},
                    "durbin_regulated_rate_indicator":             {"start": 158, "end": 159},
                    "cash_access_only_participating_indicator":    {"start": 159, "end": 160},
                    "authenticator_indicator":                     {"start": 160, "end": 161},
                    "issuer_target_market_participation_indicator":{"start": 162, "end": 163},
                    "post_date_service_indicator":                 {"start": 163, "end": 164},
                    "meal_voucher_indicator":                      {"start": 164, "end": 165},
                    "non_reloadable_prepaid_switch":               {"start": 165, "end": 167},
                    "faster_funds_indicator":                      {"start": 167, "end": 168},
                    "anonymous_prepaid_indicator":                 {"start": 168, "end": 169},
                    "cardholder_currency_indicator":               {"start": 169, "end": 170},
                    "pay_by_account_indicator":                    {"start": 170, "end": 171},
                    "issuer_account_range_gaming_participation_indicator": {"start": 171, "end": 172},
                }
            },
        }
        return params
    

# ══════════════════════════════════════════════════════════════════════════════
# 4. LÓGICA DE NEGOCIO (parsing DE, encoding, headers)
# ══════════════════════════════════════════════════════════════════════════════

def obtain_encoding(db: Database, client_id: str, file_id: str) -> str | None:
    df_file_control = db.read_sql(
        """
        SELECT file_type
        FROM file_control
        WHERE upper(client_id) = upper(?)
        AND upper(file_id) = upper(?)
        """,
        params=(client_id, file_id),
    )
 
    file_type = str(df_file_control["file_type"].iloc[0]).strip().upper()
 
    if file_type == "IN":
        col = "file_mc_encoding_in"
    elif file_type == "OUT":
        col = "file_mc_encoding_out"
    else:
        return None
 
    df_client = db.read_sql(
        f"""
        SELECT {col}
        FROM client
        WHERE upper(client_id) = upper(?)
        """,
        params=(client_id,),
    )
 
    file_mc_encoding = str(df_client[col].iloc[0]).strip().upper()
 
    if file_mc_encoding in ("LATIN-1", "LATIN1", "ISO-8859-1", "ASCII"):
        return "Latin-1"
    elif file_mc_encoding in ("CP500", "EBCDIC", "EBDIC_DIGITS"):
        return "cp500"
    else:
        return None
 
 
DEFAULT_NUMERIC_DES: frozenset[int] = frozenset({
    2, 3, 4, 5, 6, 9, 10, 12, 14, 23, 24, 25, 26, 30, 37, 38, 49, 50, 51, 71, 73, 93, 94, 95, 100
})
DEFAULT_BINARY_DES: frozenset[int]      = frozenset({55})
DEFAULT_EBCDIC_TEXT_DES: frozenset[int] = frozenset({43, 48, 22})
 
DE_COL: Dict[int, str] = {de: f"de_{de}" for de in range(2, 129)}


def parse_des_one_pass(
    body: bytes, fields: list[int], enc: str, de_spec: dict, *, max_de: int = 128
) -> Dict[int, bytes]:
    if not body or not fields:
        return {}
 
    pos = 0
    out: Dict[int, bytes] = {}
    de_get = de_spec.get
 
    for de in fields:
        if de < 2:
            continue
        if de > max_de:
            break
 
        cfg = de_get(de)
        if not cfg:
            break
 
        length = cfg["length"]
 
        if cfg["fixed"]:
            ln = int(length)
            if pos + ln > len(body):
                break
            raw = body[pos:pos + ln]
            pos = pos + ln
        else:
            len_digits = int(length)
            if pos + len_digits > len(body):
                break
            raw_len = body[pos:pos + len_digits]
            pos = pos + len_digits
            try:
                ln = int(decode_digits(raw_len, enc).strip())
            except ValueError:
                break
            if pos + ln > len(body):
                break
            raw = body[pos:pos + ln]
            pos = pos + ln
 
        out[de] = raw
    return out
 
 
def decode_text_best(raw: bytes, enc: str) -> str:
    """Si el MTI fue EBCDIC_DIGITS, usa cp500; si no, ascii/latin1."""
    if enc == "EBCDIC_DIGITS":
        return raw.decode("cp500", errors="replace")
    try:
        return raw.decode("ascii", errors="replace")
    except UnicodeDecodeError:
        return raw.decode("latin1", errors="replace")
 
 
def format_de_value(
    de: int,
    raw: Optional[bytes],
    enc: str,
    *,
    numeric_des: AbstractSet[int] = DEFAULT_NUMERIC_DES,
    binary_des: AbstractSet[int]  = DEFAULT_BINARY_DES,
    ebcdic_text_des: AbstractSet[int] = DEFAULT_EBCDIC_TEXT_DES,
) -> Optional[str]:
    if raw is None:
        return None
    if de in binary_des:
        return raw.hex()
    if de in numeric_des:
        return decode_digits(raw, enc).strip()
    return decode_text_best(raw, enc)


def build_wide_row(
    *,
    msg_no: int,
    block: Optional[int],
    mti: Optional[str],
    enc: Optional[str],
    function_code: Optional[str],
    function_role: Optional[str],
    parse_ok: bool,
    bitmap_hex: Optional[str],
    body_hex: Optional[str],
    de_spec: dict,
    fields: Optional[list[int]] = None,
    numeric_des: AbstractSet[int]     = DEFAULT_NUMERIC_DES,
    binary_des: AbstractSet[int]      = DEFAULT_BINARY_DES,
    ebcdic_text_des: AbstractSet[int] = DEFAULT_EBCDIC_TEXT_DES,
    unknown_mode: str = "skip",  # "skip" | "hex" | "bytes"
):
    """
    Convierte un row base (con body_hex/bitmap_hex) a row wide con columnas de data elements.
    """
    base = {
        "file_idn":      None,
        "file_dt":       None,
        "msg_no":        msg_no,
        "block":         block,
        "mti":           mti,
        "enc":           enc,
        "function_code": function_code,
        "function_role": function_role,
        "parse_ok":      parse_ok,
    }
 
    if (not parse_ok) or (body_hex is None) or (not enc) or (bitmap_hex is None):
        return base
 
    if isinstance(body_hex, (bytes, bytearray)):
        body = bytes(body_hex)
    elif isinstance(body_hex, str):
        body = bytes.fromhex(body_hex)
    else:
        return base
 
    if isinstance(bitmap_hex, (bytes, bytearray)):
        bitmap = bytes(bitmap_hex)
    elif isinstance(bitmap_hex, str):
        bitmap = bytes.fromhex(bitmap_hex)
    else:
        return base
 
    if fields is None:
        fields = bitmap_bits(bitmap=bitmap)
 
    pos    = 0
    de_get = de_spec.get
    cols   = DE_COL
    num    = numeric_des
    bin_   = binary_des
    txt_   = ebcdic_text_des
 
    for de in fields:
        if de < 2:
            continue
        if de > 128:
            break
 
        cfg = de_get(de)
        if not cfg:
            break
 
        length = int(cfg["length"])
 
        if cfg["fixed"]:
            ln = length
        else:
            if pos + length > len(body):
                break
            raw_len = body[pos:pos + length]
            pos = pos + length
            try:
                ln = int(decode_digits(raw_len, enc).strip())
            except ValueError:
                break
 
        if pos + ln > len(body):
            break
 
        raw = body[pos:pos + ln]
        pos = pos + ln
 
        col = cols[de]
        if de in bin_:
            base[col] = raw.hex()
        elif de in num:
            base[col] = decode_digits(raw, enc).strip()
        elif de in txt_:
            base[col] = decode_text_best(raw, enc)
        else:
            if unknown_mode == "hex":
                base[col] = raw.hex()
            elif unknown_mode == "bytes":
                base[col] = raw
            else:
                base[col] = decode_text_best(raw, enc)
 
    return base


def extract_de24_fast(
    body_hex: Any,
    bitmap_hex: Any,
    enc: Any,
    de_spec: dict,
    fields: Optional[list[int]],
) -> str | None:
    if (body_hex is None) or (bitmap_hex is None) or (not enc):
        return None
 
    body   = bytes(body_hex)   if isinstance(body_hex,   (bytes, bytearray)) else bytes.fromhex(body_hex)
    bitmap = bytes(bitmap_hex) if isinstance(bitmap_hex, (bytes, bytearray)) else bytes.fromhex(bitmap_hex)
 
    if fields is None:
        fields = bitmap_bits(bitmap)
 
    raw_map = parse_des_one_pass(body=body, fields=fields, enc=enc, de_spec=de_spec, max_de=24)
    raw24   = raw_map.get(24)
    if raw24 is None:
        return None
 
    return decode_digits(raw24, enc).strip()
 
 
def extract_pds_value_48_105(pds_blob: str | None, target_tag: str = "0105") -> str | None:
    if pds_blob is None or pd.isna(pds_blob):
        return None
 
    s = str(pds_blob)
    i = 0
    n = len(s)
 
    while i + 7 <= n:
        tag = s[i:i + 4]
        try:
            ln = int(s[i + 4:i + 7])
        except ValueError:
            return None
 
        start = i + 7
        end   = start + ln
        if end > n:
            return None
 
        val = s[start:end]
        if tag == target_tag:
            return val
 
        i = end
 
    return None
 
 
def add_headers_fields_697(df: pd.DataFrame) -> None:
    """
    Para trailers (function_code == '695'):
      file_idn = valor del PDS 0105 dentro de de_48
      file_dt  = SUBSTRING(file_idn, 4, 6)  → slice [3:9]
    Modifica el DataFrame IN-PLACE.
    """
    mask = df["function_code"].astype(str).eq("695")
    if not mask.any():
        return
 
    s = df.loc[mask, "de_48"].astype("string")
    df.loc[mask, "file_idn"] = s.map(lambda x: extract_pds_value_48_105(x, "0105"))
    df.loc[mask, "file_dt"]  = df.loc[mask, "file_idn"].astype("string").str.slice(3, 9)
 
 
def apply_block_file_context_697(
    df: pd.DataFrame,
    *,
    state: dict[int, tuple[str, str]],
    strict: bool = False,
) -> None:
    """
    Aplica el contexto file_idn/file_dt a todas las filas del DataFrame
    según el header 697 y el block. Modifica df IN-PLACE.
    """
    hdr = df["function_code"].astype("string").str.strip().eq("695")
    if hdr.any():
        h = (
            df.loc[hdr, ["block", "file_idn", "file_dt"]]
            .dropna(subset=["block", "file_idn"])
        )
        if not h.empty:
            h["block"] = h["block"].astype(int)
            dup = h["block"][h["block"].duplicated(keep=False)]
            if not dup.empty:
                blocks = sorted(dup.unique().tolist())
                msg = f"Más de un header 697 para block(s): {blocks}"
                if strict:
                    raise ValueError(msg)
                else:
                    print(f"[WARN] {msg}")
 
            new   = ~h["block"].isin(state.keys())
            h_new = h.loc[new]
            if not h_new.empty:
                state.update(
                    dict(
                        zip(
                            h_new["block"].to_list(),
                            zip(
                                h_new["file_idn"].astype(str).to_list(),
                                h_new["file_dt"].astype(str).to_list(),
                            ),
                        )
                    )
                )
 
    m = df["block"].notna()
    if m.any() and state:
        pre = df.loc[m, "block"].astype(int).map(state)
        ok  = pre.notna()
        if ok.any():
            idx  = pre.index[ok]
            vals = pre.loc[idx].tolist()
            df.loc[idx, "file_idn"] = [v[0] for v in vals]
            df.loc[idx, "file_dt"]  = [v[1] for v in vals]


def decode_numeric(text_bytes: bytes, enc: str) -> str:
    return decode_digits(text_bytes, enc).strip()
 
 
def decode_text_ebcdic(raw: bytes) -> str:
    try:
        return raw.decode("cp500").strip()
    except Exception:
        return raw.decode("latin1", errors="replace").strip()


def decode_text(raw: bytes, enc: str) -> str:
    if not raw:
        return ""
    if enc == "EBCDIC_DIGITS":
        try:
            return raw.decode("cp500", errors="replace").strip()
        except Exception:
            return raw.decode("latin1", errors="replace").strip()
    try:
        return raw.decode("ascii").strip()
    except UnicodeDecodeError:
        return raw.decode("latin1", errors="replace").strip()


def extract_one_de_from_body(
    body: bytes,
    fields: list[int],
    enc: str,
    de_spec: dict,
    target_de: int,
    *,
    max_de: int = 128,
    numeric_des: Optional[set[int]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Extrae SOLO el DE target_de consumiendo el body según de_spec.
    Retorna dict: {"de", "raw", "raw_hex", "len", "text"} o None.
    """
    if not body or not fields:
        return None
 
    if numeric_des is None:
        numeric_des = {
            2, 3, 4, 5, 6, 9, 10, 12, 14, 22, 23, 24, 25, 26, 30, 37, 38, 40, 41, 42,
            49, 50, 51, 71, 73
        }
 
    present = sorted([f for f in fields if 2 <= f <= max_de])
 
    if target_de not in present:
        return None
 
    pos = 0
    n   = len(body)
 
    for de in present:
        cfg = de_spec.get(de)
        if cfg is None:
            return None
 
        if cfg["fixed"]:
            ln = int(cfg["length"])
            if pos + ln > n:
                return None
            raw = body[pos:pos + ln]
            pos += ln
        else:
            len_digits = int(cfg["length"])
            if pos + len_digits > n:
                return None
            raw_len = body[pos:pos + len_digits]
            pos += len_digits
            ln_str = decode_numeric(raw_len, enc)
            if not ln_str.isdigit():
                return None
            ln = int(ln_str)
            if pos + ln > n:
                return None
            raw = body[pos:pos + ln]
            pos += ln
 
        if de == target_de:
            text = decode_numeric(raw, enc) if de in numeric_des else decode_text(raw, enc)
            return {"de": de, "raw": raw, "raw_hex": raw.hex(), "len": len(raw), "text": text}
 
    return None


# ══════════════════════════════════════════════════════════════════════════════
# 5. IO / LECTORES DE MENSAJES
# ══════════════════════════════════════════════════════════════════════════════

def unblock_1014(
    stream_file: BinaryIO,
    payload_size: int = 1012,
    sep_size: int = 2,
    valid_seps: tuple[bytes, ...] = (b"", b"\x20\x20", b"\x40\x40", b"\x00\x00"),
) -> bytes:
    stream_file.seek(0)
    out_bytes = bytearray()
 
    while True:
        chunk = stream_file.read(payload_size)
        if chunk:
            out_bytes.extend(chunk)
        if len(chunk) < payload_size:
            break
 
        sep = stream_file.read(sep_size)
        if sep not in valid_seps:
            stream_file.seek(stream_file.tell() - len(sep))
 
    return bytes(out_bytes)
 
 
def read_len_prefixed_messages(
    stream,
    *,
    as_hex: bool = True,
    client_id: str,
    file_id: str,
    db: Database,
    encoding: str,
):
    """
    Lee [4 bytes len] + [payload].
    Devuelve rows con body/bitmap en HEX o bytes según as_hex.
    """
    pos    = 0
    msg_no = 0
 
    while True:
        raw_len = stream.read(4)
        if len(raw_len) < 4:
            break
        msg_len = struct.unpack(">i", raw_len)[0]
        if msg_len <= 0:
            break
        payload = stream.read(msg_len)
        if len(payload) < msg_len:
            break
 
        msg_no += 1
        mti, enc = detect_mti(payload=payload, encoding=encoding)
        parts    = split_mti_bitmap_body(payload=payload)
 
        if parts is None:
            row: Dict[str, Any] = {
                "msg_no":   msg_no,
                "offset":   pos,
                "msg_len":  msg_len,
                "mti":      mti,
                "enc":      enc,
                "parse_ok": False,
            }
            if as_hex:
                row["bitmap_hex"] = None
                row["body_hex"]   = payload.hex()
            else:
                row["bitmap"] = None
                row["body"]   = payload
        else:
            mti_bytes, bitmap, body, fields, has_secondary = parts
            row = {
                "msg_no":   msg_no,
                "offset":   pos,
                "msg_len":  msg_len,
                "mti":      mti,
                "enc":      enc,
                "parse_ok": True,
                "fields":   fields,
            }
            if as_hex:
                row["bitmap_hex"] = bitmap.hex()
                row["body_hex"]   = body.hex()
            else:
                row["bitmap"] = bitmap
                row["body"]   = body
        yield row
        pos = pos + 4 + msg_len
 
 
def _bitmap_to_fields_1_128(bitmap_16: bytes) -> List[int]:
    fields = []
    de_no  = 1
    for byte in bitmap_16:
        for bit in range(7, -1, -1):
            if (byte >> bit) & 1:
                fields.append(de_no)
            de_no += 1
    return fields


# MTIs validos que este pipeline procesa (ver subdir_for_mti). Usados por
# _resync_stream para reconocer el inicio del siguiente mensaje cuando el
# mensaje actual queda corrupto/desincronizado.
_RESYNC_MTIS = ("1240", "1442", "1644", "1740")


def _valid_mti_byte_patterns(encoding: str) -> set[bytes]:
    """
    Devuelve el set de secuencias de 4 bytes que representan un MTI valido
    de _RESYNC_MTIS, codificadas segun `encoding` (cp500/EBCDIC o latin-1/ASCII).
    No usa detect_mti() para evitar excepciones de decode sobre bytes arbitrarios
    durante el escaneo de resync.
    """
    if encoding.upper() in ("CP500", "EBCDIC", "EBCDIC_DIGITS"):
        return {bytes(0xF0 + int(d) for d in mti) for mti in _RESYNC_MTIS}
    return {mti.encode("ascii") for mti in _RESYNC_MTIS}


def _resync_stream(stream: BinaryIO, encoding: str, scan_limit: int = 50000):
    """
    Escanea hacia adelante desde la posicion actual del stream buscando el
    inicio del siguiente mensaje valido: 4 bytes de record_length en rango
    plausible (20-65535) seguidos de un MTI reconocido (_RESYNC_MTIS).

    Mismo mecanismo que el sistema legacy (tst_files/mcfiles.py -> _resync_stream),
    usado para descartar un mensaje corrupto y continuar leyendo el resto del
    archivo, en vez de abortar la interpretacion completa.

    Retorna (True, nueva_posicion) si encuentra un punto de resync dentro de
    scan_limit bytes, o (False, None) si no lo encuentra (deja el stream en
    su posicion original).
    """
    start = stream.tell()
    end = stream.getbuffer().nbytes
    valid_mtis = _valid_mti_byte_patterns(encoding)

    for offset in range(scan_limit):
        scan_pos = start + offset
        if scan_pos + 8 > end:
            break
        stream.seek(scan_pos)
        rl_bytes = stream.read(4)
        if len(rl_bytes) < 4:
            break
        try:
            potential_rl = struct.unpack(">i", rl_bytes)[0]
        except Exception:
            continue
        if not (20 <= potential_rl <= 65535):
            continue
        if stream.read(4) in valid_mtis:
            stream.seek(scan_pos)
            return True, scan_pos

    stream.seek(start)
    return False, None


def read_len_prefixed_messages_variable(
    stream: BinaryIO,
    *,
    as_hex: bool = False,
    client_id: str,
    file_id: str,
    db: Database,
    encoding: str,
):
    """
    Reader estilo estructura ISO, no depende del msg_len:
      - Lee 4 bytes length (solo control)
      - Lee 20 bytes (MTI 4 + bitmap 16)
      - Lee DEs según bitmap y parameters (fixed/variable) con encoding cp500

    Si la lectura de un mensaje queda corrupta/desincronizada (DE no definido
    en `parameters`, longitud variable no numerica, o short read), el mensaje
    se descarta y se intenta un resync hacia el siguiente mensaje valido
    (ver _resync_stream). Si el resync falla, se detiene la lectura.

    Retorna rows con bitmap/body (bytes o hex).
    """
    parameters = Parameters().getdataelements()
    msg_no     = 0
    base0      = stream.tell()

    while True:
        msg_start = stream.tell()
        raw_len   = stream.read(4)
        if len(raw_len) < 4:
            break
        try:
            record_length = struct.unpack(">i", raw_len)[0]
        except Exception:
            record_length = 0
        if record_length == 0:
            break

        message_total = stream.read(20)
        if len(message_total) != 20:
            break

        mti_bytes, bitmap_16 = struct.unpack("4s16s", message_total)
        mti, enc             = detect_mti(payload=mti_bytes, encoding=encoding)
        fields_present       = _bitmap_to_fields_1_128(bitmap_16)
        body_bytes           = bytearray()
        parse_ok             = True

        log.logger.info(f"mti_bytes: {mti_bytes} | bitmap_16: {bitmap_16} | mti: {mti} | enc: {enc}")
        log.logger.info(f"field_present: {fields_present} | body_bytes: {body_bytes} | parse_ok: {parse_ok}")

        for i in range(2, 129):
            if i not in fields_present:
                continue
            de_spec = parameters.get(i)
            if de_spec is None:
                parse_ok = False
                break
            if de_spec["fixed"]:
                de_len = de_spec["length"]
                v = stream.read(de_len)
                if len(v) < de_len:
                    parse_ok = False
                    break
                body_bytes.extend(v)
            else:
                len_digits = de_spec["length"]
                raw_num    = stream.read(len_digits)
                if len(raw_num) < len_digits:
                    parse_ok = False
                    break
                try:
                    de_len = int(raw_num.decode(encoding))
                except Exception:
                    parse_ok = False
                    break

                v = stream.read(de_len)
                if len(v) < de_len:
                    parse_ok = False
                    break

                body_bytes.extend(raw_num)
                body_bytes.extend(v)

        if not parse_ok:
            resynced, new_pos = _resync_stream(stream, encoding=encoding)
            if resynced:
                log.logger.warning(
                    f"[mc-interpreter] Mensaje corrupto descartado en offset "
                    f"{msg_start - base0} (mti={mti!r}, record_length={record_length}) "
                    f"-- RESYNC exitoso en offset {new_pos - base0}, continuando."
                )
                continue
            else:
                log.logger.warning(
                    f"[mc-interpreter] Mensaje corrupto en offset {msg_start - base0} "
                    f"(mti={mti!r}, record_length={record_length}) -- RESYNC fallido, "
                    f"deteniendo lectura. Mensajes leidos hasta ahora: {msg_no}."
                )
                break

        msg_no += 1
        has_secondary = (bitmap_16[0] & 0x80) != 0
 
        row: Dict[str, Any] = {
            "msg_no":       msg_no,
            "offset":       msg_start - base0,
            "record_len":   record_length,
            "mti":          str(mti),
            "enc":          enc,
            "parse_ok":     parse_ok,
            "fields":       fields_present,
            "has_secondary": has_secondary,
        }
 
        if as_hex:
            row["bitmap_hex"] = bitmap_16.hex()
            row["body_hex"]   = bytes(body_bytes).hex()
        else:
            row["bitmap"] = bitmap_16
            row["body"]   = bytes(body_bytes)
 
        yield row
 
 
# ══════════════════════════════════════════════════════════════════════════════
# 6. STORAGE / PARQUET WRITERS
# ══════════════════════════════════════════════════════════════════════════════
 
def _canonical_schema_from_de_spec(de_spec: dict) -> pa.Schema:
    fields = [
        pa.field("file_idn",      pa.string()),
        pa.field("file_dt",       pa.string()),
        pa.field("file_id",       pa.string()),
        pa.field("content_hash",  pa.string()),
        pa.field("msg_no",        pa.int64()),
        pa.field("block",         pa.int64()),
        pa.field("mti",           pa.string()),
        pa.field("enc",           pa.string()),
        pa.field("function_code", pa.string()),
        pa.field("function_role", pa.string()),
        pa.field("parse_ok",      pa.bool_()),
    ]
    for de in sorted(de_spec.keys()):
        fields.append(pa.field(f"de_{de}", pa.string()))
    return pa.schema(fields)


def _ensure_and_cast(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """
    Alinea la tabla al schema canonical: agrega columnas faltantes como nulls,
    reordena y castea tipos. Una sola construcción de tabla al final (sin
    tablas intermedias por iteración).
    """
    present = set(table.schema.names)
    nrows   = table.num_rows
 
    arrays = []
    for field in schema:
        if field.name in present:
            col = table.column(field.name)
            if col.type != field.type:
                col = col.cast(field.type, safe=False)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(nrows, type=field.type))
 
    return pa.table(arrays, schema=schema)
 
 
def subdir_for_mti(mti: str) -> str:
    mti = str(mti)
    if mti == "1240":
        return "100_IPM_1240_RAW"
    elif mti == "1442":
        return "100_IPM_1442_RAW"
    elif mti == "1644":
        return "100_IPM_1644_RAW"
    elif mti == "1740":
        return "100_IPM_1740_RAW"
    return "100_IPM_UNK_RAW"
 
 
def _base_dir_for_subdir(fs: FileStorage, layer, client_id: str, file_id: str, subdir: str) -> Path:
    base = Path(fs._get_file_path(layer, client_id, file_id, subdir=subdir))
    return base.parent
 
 
def write_parquet_by_mti_block_streaming(
    df_chunk: pd.DataFrame,
    *,
    fs: FileStorage,
    target_layer,
    client_id: str,
    file_id: str,
    schema: pa.Schema,
    writers: dict,
) -> None:
    df_chunk = df_chunk[df_chunk["file_idn"].notna()]
    if df_chunk.empty:
        return
 
    for (file_idn, mti), g in df_chunk.groupby(["file_idn", "mti"], sort=False):
        file_idn = str(file_idn)
        mti_s    = str(mti)
        subdir   = subdir_for_mti(mti_s)
 
        base_dir = _base_dir_for_subdir(fs, target_layer, client_id, file_id, subdir)
        base_dir.mkdir(parents=True, exist_ok=True)
 
        filename = f"{file_id}_{file_idn}_{mti_s}.parquet"
        out_path = base_dir / filename
        key      = (file_id, file_idn, mti_s)
 
        table = pa.Table.from_pandas(g, preserve_index=False)
        table = _ensure_and_cast(table, schema)
 
        if key not in writers:
            writers[key] = pq.ParquetWriter(
                out_path.as_posix(),
                schema,
                compression="snappy",
                use_dictionary=True,
            )
 
        writers[key].write_table(table)


def finalize_writers(writers: Dict[Tuple[str, int, str], pq.ParquetWriter]) -> None:
    for w in writers.values():
        w.close()
    writers.clear()
 
 
def write_parquet_by_mti_block_streaming2(
    df_chunk: pd.DataFrame,
    *,
    fs: FileStorage,
    target_layer,
    client_id: str,
    file_id: str,
    schema: pa.Schema,
    writers: dict,
) -> None:
    df_chunk = df_chunk[df_chunk["block"].notna()]
    if df_chunk.empty:
        return
 
    for (block, mti), g in df_chunk.groupby(["block", "mti"], sort=False):
        block_i = int(block)
        mti_s   = str(mti)
        subdir  = subdir_for_mti(mti_s)
 
        base_dir = _base_dir_for_subdir(fs, target_layer, client_id, file_id, subdir)
        base_dir.mkdir(parents=True, exist_ok=True)
 
        filename = f"{file_id}_{block_i}_{mti_s}.parquet"
        out_path = base_dir / filename
        key      = (file_id, block_i, mti_s)
 
        table = pa.Table.from_pandas(g, preserve_index=False)
        table = _ensure_and_cast(table, schema)
 
        if key not in writers:
            writers[key] = pq.ParquetWriter(
                out_path.as_posix(),
                schema,
                compression="snappy",
                use_dictionary=True,
            )
 
        writers[key].write_table(table)
 
 
def extract_fc_from_filepath(filepath: str | Path) -> str:
    name = Path(filepath).name
    return name.rsplit("_", 1)[-1].replace(".parquet", "")


# ══════════════════════════════════════════════════════════════════════════════
# INICIALIZACIÓN DE MÓDULO
# ══════════════════════════════════════════════════════════════════════════════
 
fs      = FileStorage()
DE_SPEC = Parameters().getdataelements()


# ══════════════════════════════════════════════════════════════════════════════
# 7. ORQUESTADOR
# ══════════════════════════════════════════════════════════════════════════════

def _load_as_binary(
    layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    subdir: str = "",
) -> BinaryIO:
    return fs.read_binary(fs.Layer.LANDING, client_id, file_id, subdir, True)
 
 
def _extract_function_code_inline(row: dict, de_spec: dict) -> Optional[str]:
    """
    Extrae el function_code (DE24) directamente de un row del generador.
    Retorna None si el mensaje no es 1644 o no es válido.
    """
    if row.get("mti") != "1644" or not row.get("parse_ok", False):
        return None
 
    return extract_de24_fast(
        body_hex=row.get("body"),
        bitmap_hex=row.get("bitmap"),
        enc=str(row.get("enc", "")),
        de_spec=de_spec,
        fields=row.get("fields"),
    )


def _process_block(
    block_buffer: list,
    block_no: int,
    file_idn: Optional[str],
    file_dt: Optional[str],
    schema: pa.Schema,
    writers: dict,
    target_layer,
    client_id: str,
    file_id: str,
    content_hash: str = "",
) -> None:
    """
    Convierte el buffer de un bloque a DataFrame wide, aplica el contexto
    file_idn/file_dt del trailer 695, y escribe el parquet correspondiente.
    Se llama una vez por bloque, justo cuando se detecta el 695.
    """
    if not block_buffer:
        log.logger.info(f"No hay block_buffer/vacio")
        return
 
    wide_rows = [
        build_wide_row(
            msg_no=cast(int, row["msg_no"]),
            block=block_no,
            mti=cast(Optional[str], row.get("mti")),
            enc=cast(Optional[str], row.get("enc")),
            function_code=cast(Optional[str], row.get("function_code")),
            function_role=row.get("function_role"),
            parse_ok=cast(bool, row.get("parse_ok", False)),
            bitmap_hex=row.get("bitmap"),
            body_hex=row.get("body"),
            de_spec=DE_SPEC,
            fields=cast(Optional[list[int]], row.get("fields")),
        )
        for row in block_buffer
    ]
 
    df_block = pd.DataFrame(wide_rows)
    del wide_rows
 
    df_block = df_block.reindex(columns=schema.names)
 
    if file_idn is not None:
        df_block["file_idn"] = file_idn
        df_block["file_dt"]  = file_dt
 
    df_block["content_hash"] = content_hash
    df_block["file_id"]      = file_id
 
    write_parquet_by_mti_block_streaming(
        df_chunk=df_block,
        fs=fs,
        target_layer=target_layer,
        client_id=client_id,
        file_id=file_id,
        schema=schema,
        writers=writers,
    )
 
    del df_block



def interpretate_msg(
    origin_layer,
    target_layer,
    client_id: str,
    file_id: str,
    origin_subdir: str = "",
    target_sub_dir: str = "",
    test_path: str = "",
    content_hash: str = "",
) -> list[str]:
    """
    Interpreta un archivo IPM Mastercard y escribe parquets clasificados por MTI y bloque.
 
    ALGORITMO (single-pass, bloque a bloque):
    ──────────────────────────────────────────
    En vez de leer todo el archivo en RAM y luego procesar (dos pasadas),
    ahora leemos mensaje a mensaje y mantenemos en RAM solo el bloque actual.
 
    Flujo por mensaje:
      - Si es 1644/697 (header): abre un nuevo bloque, inicia el buffer
      - Si es 1644/695 (trailer): extrae file_idn/file_dt, procesa el bloque
                                   completo, y LIMPIA el buffer → libera RAM
      - Cualquier otro mensaje: se agrega al buffer del bloque activo
 
    Resultado de RAM:
      ANTES: payloads de TODOS los mensajes simultáneamente (~2 GB)
      AHORA: buffer de UN BLOQUE (~2-5 MB típico)
    """
 
    # 0) Limpiar /tmp de posibles ejecuciones previas en un entorno Lambda warm
    fs.cleanup_tmp_outputs(client_id=client_id, file_id=file_id)
 
    # 1) Leer el archivo binario desde S3 LANDING
    stream_file = _load_as_binary(origin_layer, client_id, file_id, subdir=origin_subdir)
 
    # 2) Conectar BD e interrogar configuración del archivo
    db                   = Database()
    need_unblock         = db.needs_unblock_for_file(client_id=client_id, file_id=file_id)
    need_interpreter_fix = db.needs_interpreter_fix(client_id=client_id, file_id=file_id)
    file_mc_encoding     = str(obtain_encoding(db=db, client_id=client_id, file_id=file_id))
 
    log.logger.info(f"Need unblock: {need_unblock} | Need_interpreter_fix: {need_interpreter_fix} | file_mc_encoding: {file_mc_encoding}")
 
    # 3) Desbloquear si es necesario y cargar en memoria
    if need_unblock:
        unblocked_bytes = unblock_1014(stream_file=stream_file)  # ~300 MB en RAM
    else:
        stream_file.seek(0)
        unblocked_bytes = stream_file.read()
 
    stream_io = io.BytesIO(unblocked_bytes)
    del unblocked_bytes  # liberamos ~300 MB
    del stream_file      # ya no se usa
    gc.collect()
 
    # 4) Obtener el generador de mensajes
    if need_interpreter_fix == True:
        rows = read_len_prefixed_messages(
            stream=stream_io,
            as_hex=False,
            client_id=client_id,
            file_id=file_id,
            db=db,
            encoding=file_mc_encoding,
        )
    elif need_interpreter_fix == False:
        rows = read_len_prefixed_messages_variable(
            stream=stream_io,
            as_hex=False,
            client_id=client_id,
            file_id=file_id,
            db=db,
            encoding=file_mc_encoding,
        )
 
    # 5) Procesar mensaje a mensaje, bloque por bloque
    schema = _canonical_schema_from_de_spec(DE_SPEC)
    writers: dict = {}
 
    current_block = 0      # número del bloque que estamos procesando
    block_open    = False  # True entre 697 y 695
    block_buffer  = []     # rows del bloque actual (se limpia al cerrar cada bloque)
 
    for row in rows:
        row["function_code"] = _extract_function_code_inline(row=row, de_spec=DE_SPEC)
        #log.logger.info(f"row: {row}")
 
        mti      = row.get("mti")
        fc       = row.get("function_code")
        parse_ok = row.get("parse_ok", False)
 
        # HEADER = 697: abrir nuevo bloque
        if mti == "1644" and fc == "697" and parse_ok:
            current_block += 1
            block_open    = True
            block_buffer  = [row]
 
        # TRAILER = 695: cerrar el bloque y liberar
        elif mti == "1644" and fc == "695" and parse_ok and block_open:
            block_buffer.append(row)
 
            wide_695 = build_wide_row(
                msg_no=cast(int, row["msg_no"]),
                block=current_block,
                mti="1644",
                enc=cast(Optional[str], row.get("enc")),
                function_code="695",
                function_role=None,
                parse_ok=True,
                bitmap_hex=row.get("bitmap"),
                body_hex=row.get("body"),
                de_spec=DE_SPEC,
                fields=cast(Optional[list[int]], row.get("fields")),
            )
            df_695 = pd.DataFrame([wide_695])
            add_headers_fields_697(df_695)
 
            file_idn = str(df_695.at[0, "file_idn"]) if "file_idn" in df_695.columns else None
            file_dt  = str(df_695.at[0, "file_dt"])  if "file_dt"  in df_695.columns else None
            del df_695, wide_695
 
            _process_block(
                block_buffer=block_buffer,
                block_no=current_block,
                file_idn=file_idn,
                file_dt=file_dt,
                schema=schema,
                writers=writers,
                target_layer=target_layer,
                client_id=client_id,
                file_id=file_id,
                content_hash=content_hash,
            )
 
            block_buffer.clear()  # ← RAM del bloque liberada aquí
            block_open = False
            pa.default_memory_pool().release_unused()
 
        # Cualquier otro mensaje: acumular en el buffer del bloque activo
        elif block_open:
            block_buffer.append(row)
 
    # 6) Cerrar writers, subir parquets temporales a S3 y limpiar memoria
    finalize_writers(writers=writers)
    uploaded_outputs = fs.upload_tmp_outputs(
        layer=target_layer,
        client_id=client_id,
        file_id=file_id,
    )
    del stream_io
    gc.collect()
    return uploaded_outputs

# ══════════════════════════════════════════════════════════════════════════════
# 8. LAMBDA HANDLER
# ══════════════════════════════════════════════════════════════════════════════

layer = FileStorage.Layer
 
 
def _normalize_event(event: Any) -> dict[str, Any]:
    """
    Acepta payload directo, payload dentro de body o payload dentro de
    variables_input, manteniendo compatibilidad con routers internos.
    """
    if event is None:
        return {}
 
    if isinstance(event, str):
        try:
            return json.loads(event)
        except json.JSONDecodeError:
            return {}
 
    if not isinstance(event, dict):
        return {}
 
    normalized = dict(event)
 
    body = normalized.get("body")
    if isinstance(body, str) and body.strip():
        try:
            body_data = json.loads(body)
            if isinstance(body_data, dict):
                normalized.update(body_data)
        except json.JSONDecodeError:
            pass
    elif isinstance(body, dict):
        normalized.update(body)
 
    variables_input = normalized.get("variables_input")
    if isinstance(variables_input, str) and variables_input.strip():
        try:
            variables_data = json.loads(variables_input)
            if isinstance(variables_data, dict):
                normalized.update(variables_data)
        except json.JSONDecodeError:
            pass
    elif isinstance(variables_input, dict):
        normalized.update(variables_input)
 
    return normalized


def _extract_event_params(
    event: dict[str, Any]
) -> tuple[str, str, str, str, str, str, str, str, str, str]:
    """
    Extrae y valida los parámetros del evento.
 
    Misma estructura de input que vi_transform.py para compatiblidad
    con el payload que emite el router hacia Step Functions:
 
        client_id, file_id, filename, s3_key_landing, bucket_landing,
        brand, brand_id, file_type, file_date, content_hash
    """
    client_id = str(event.get("client_id", "")).strip()
    file_id = str(event.get("file_id", event.get("content_hash", ""))).strip()
    brand = str(event.get("brand", "")).strip()
    brand_id = str(event.get("brand_id", "")).strip()
    file_type = str(event.get("file_type", "")).strip()
    file_date = str(event.get("file_date")).strip()
    content_hash = str(event.get("content_hash")).strip()
    s3_key_landing = str(event.get("s3_key_landing")).strip()
    bucket_landing = str(event.get("bucket_landing")).strip()
    filename = str(event.get("filename")).strip()
 
    missing = []
    if not client_id:
        missing.append("client_id")
    if not file_id:
        missing.append("file_id")
 
    if missing:
        raise ValueError(
            f"Payload inválido - faltan campos obligatorios: {', '.join(missing)}. "
            f"Event recibido: {json.dumps(event, default=str)}"
        )
 
    return (
        client_id, file_id, brand, brand_id, 
        file_type, file_date, content_hash, 
        s3_key_landing, bucket_landing, filename
    )


def _pipeline_mc_interpreter(client_id: str, file_id: str, content_hash: str = "") -> list[str]:
    """
    Orquesta únicamente la etapa interpreter Mastercard.
    Equivalente Lambda al llamado local:
        interpretate_msg(layer.LANDING, layer.STAGING, client_id, file_id)
    """
    return interpretate_msg(
        origin_layer=layer.LANDING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
        content_hash=content_hash,
    )


# Captura el MTI de segmentos tipo "100_IPM_1240_RAW", "400_IPM_1644_CLN", etc.
_MTI_FROM_KEY_RE = re.compile(r"/\d+_IPM_(\d{4})_\w+/")


def _build_outputs_for_calculate(s3_urls: list[str]) -> list[dict]:
    """
    Transforma la lista de URLs S3 completas que produce _pipeline_mc_interpreter
    en el array de objetos que consume mc_calculate como parámetro --outputs.
 
    Input:
        [
            "s3://bucket/SBSA/MC/100_IPM_1240_RAW/file_type=IN/date=2026-02-18/xxx_1240.parquet",
            ...
        ]
 
    Output:
        [
            {"mti": "1240", "s3_key": "SBSA/MC/100_IPM_1240_RAW/file_type=IN/date=2026-02-18/xxx_1240.parquet"},
            ...
        ]
    """
    result = []
    for url in s3_urls:
        # Quitar "s3://bucket/" para quedarse solo con el s3_key relativo
        if url.startswith("s3://"):
            without_scheme = url[5:]                    # "bucket/rest/of/key"
            s3_key = without_scheme.split("/", 1)[1]    # "rest/of/key"
        else:
            s3_key = url
 
        m = _MTI_FROM_KEY_RE.search(url)
        mti = m.group(1) if m else "UNKNOWN"
 
        result.append({"mti": mti, "s3_key": s3_key})
 
    return result


def lambda_handler(event, context):
    """
    Entry point de AWS Lambda.
 
    Input (desde Step Functions) — mismo payload que emite el router:
    {
        "client_id":      "EURBGR",
        "file_id":        "db7f1de4075536e2bae5d1d6a4f22c75",
        "filename":       "MC.EURBGR.IPM.20260103.001",
        "s3_key_landing": "EURBGR/MC.EURBGR.IPM.20260103.001",
        "bucket_landing": "itx-landing-dev",
        "brand":          "MASTERCARD",
        "brand_id":       "MC",
        "file_type":      "IN",
        "file_date":      "2026-01-03",
        "content_hash":   "db7f1de4075536e2bae5d1d6a4f22c75"
    }
 
    Output (para el siguiente paso en Step Functions):
    {
        "status":         "SUCCESS",
        "total_outputs":  27,
        "total_records":  0,
        "outputs":        [{"mti": "1240", "s3_key": "SBSA/MC/100_IPM_1240_RAW/..."}, ...],
        "client_id":      "EURBGR",
        "file_id":        "db7f1de4...",
        "brand":          "MASTERCARD",
        "brand_id":       "MC",
        "file_type":      "IN",
        "file_date":      "2026-01-03",
        "content_hash":   "db7f1de4...",
        "s3_key_landing": "EURBGR/MC.EURBGR.IPM.20260103.001",
        "bucket_landing": "itx-landing-dev",
        "filename":       "MC.EURBGR.IPM.20260103.001"
    }
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
 
    event = _normalize_event(event)
    logger.info("=== Inicio pipeline Mastercard Interpreter ===")
    logger.info(f"Event recibido: {json.dumps(event, default=str)}")
 
    try:
        (
            client_id, file_id, brand, brand_id, 
            file_type, file_date, content_hash,
            s3_key_landing, bucket_landing, filename,
        ) = _extract_event_params(event)
    except ValueError as exc:
        logger.error(f"Payload inválido: {exc}")
        return {
            "status":    "ERROR",
            "error":     str(exc),
            "client_id": event.get("client_id", ""),
            "file_id":   event.get("file_id",   ""),
        }
 
    logger.info(
        f"Iniciando MC Interpreter | "
        f"client_id={client_id} | "
        f"file_id={file_id} | "
        f"filename={filename} | "
        f"s3_key_landing={s3_key_landing}"
    )
 
    try:
        uploaded_outputs = _pipeline_mc_interpreter(
            client_id=client_id,
            file_id=file_id,
            content_hash=content_hash,
        )
        outputs          = _build_outputs_for_calculate(uploaded_outputs)
 
        logger.info(
            f"=== Pipeline Mastercard Interpreter completado exitosamente | "
            f"outputs={len(outputs)} ==="
        )
        logger.info(f"outputs para mc_calculate: {json.dumps(outputs)}")
 
        return {
            "status":         "SUCCESS" if outputs else "ERROR",
            "total_outputs":  len(outputs),
            "total_records":  0,
            "outputs":        outputs,
            "client_id":      client_id,
            "file_id":        file_id,
            "brand":          brand,
            "brand_id":       brand_id,
            "file_type":      file_type,
            "file_date":      file_date,
            "content_hash":   content_hash,
            "s3_key_landing": s3_key_landing,
            "bucket_landing": bucket_landing,
            "filename":       filename,
        }
 
    except Exception as exc:
        logger.error(f"Error en pipeline Mastercard Interpreter: {exc}", exc_info=True)
        return {
            "status":         "ERROR",
            "error":          str(exc),
            "client_id":      client_id,
            "file_id":        file_id,
            "brand":          brand,
            "brand_id":       brand_id,
            "file_type":      file_type,
            "file_date":      file_date,
            "content_hash":   content_hash,
            "s3_key_landing": s3_key_landing,
            "bucket_landing": bucket_landing,
            "filename":       filename,
        }