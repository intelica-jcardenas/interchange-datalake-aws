"""
file.py

Capa de acceso a S3 del motor de reglas ARDEF de Visa. Reemplaza el acceso a
filesystem local del prototipo por lectura/escritura directa a los buckets
de landing/staging/operational/reference, resolviendo internamente el
bucket y la S3 key de cada capa a partir del file_id (vía
`ardef.persistence.database.Database`).
"""

import io
import os 
from enum import StrEnum, auto
 
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
 
from ardef.logs.logger import Logger
from ardef.persistence.database import Database
 
log = Logger(__name__)
 
 
class _Layer(StrEnum):
    """
    Identifica cada una de las 4 capas S3 del pipeline (landing, staging,
    operational, reference) usadas por `FileStorage` para resolver el bucket
    y la estructura de key correspondiente.
    """
    LANDING = auto()
    STAGING = auto()
    OPERATIONAL = auto()
    REFERENCE = auto() # Bucket de datos de referencia (tabla maestra de ARDEF)
 
 
class FileStorage:
    """
    Capa de I/O sobre S3. Reemplaza el acceso a filesystem local.
 
    Buckets por capa:
        LANDING     ->  ITX_S3_BUCKET_LANDING       (default: itl-0004-itx-dev-poc-02-landing)
        STAGING     ->  ITX_S3_BUCKET_STAGING       (default: itl-0004-itx-dev-poc-02-staging)
        OPERATIONAL ->  ITX_S3_BUCKET_OPERATIONAL   (default: itl-0004-itx-dev-poc-02-operational)
        REFERENCE   ->  ITX_S3_BUCKET_REFERENCE     (default: itl-0004-itx-dev-poc-02-reference)
 
    Estructura de keys en cada bucket:
        LANDING:        {client_id}/{landing_file_name}
        STAGING:        {client_id}/{brand_id}/{file_type}/{date}/{subdir}/{file_id}.parquet
        OPERATIONAL:    {client_id}/{brand_id}/{file_type}/{date}/{subdir}/{file_id}.parquet
        REFERENCE:       visa_ardef/lu_ardef.parquet (ruta fija para la tabla maestra ARDEF)
    """
 
    Layer = _Layer
 
    def __init__(self) -> None:
        self._s3 = None
 
    def _get_client(self):
        """
        Devuelve el cliente boto3 S3, creándolo perezosamente en la primera
        llamada y reutilizándolo en las siguientes.

        Returns:
            El cliente `boto3.client("s3", ...)` cacheado en la instancia.

        Ejemplo:
            client = fs._get_client()
        """
        if self._s3 is None:
            self._s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-south-2"))
        return self._s3
    
    def _get_bucket(self, layer: _Layer) -> str:
        """
        Resuelve el nombre de bucket S3 real para una capa, leyendo la
        variable de entorno correspondiente (con default de desarrollo si
        no está seteada).

        Args:
            layer: capa del pipeline (`FileStorage.Layer.LANDING/STAGING/OPERATIONAL/REFERENCE`).

        Returns:
            Nombre del bucket S3, ej. "itl-0004-itx-dev-poc-02-landing".

        Ejemplo:
            fs._get_bucket(FileStorage.Layer.STAGING)  # "itl-0004-itx-dev-poc-02-staging"
        """
        mapping = {
            self.Layer.LANDING: ("ITX_S3_BUCKET_LANDING", "itl-0004-itx-dev-poc-02-landing"),
            self.Layer.STAGING: ("ITX_S3_BUCKET_STAGING", "itl-0004-itx-dev-poc-02-staging"),
            self.Layer.OPERATIONAL: ("ITX_S3_BUCKET_OPERATIONAL", "itl-0004-itx-dev-poc-02-operational"),
            self.Layer.REFERENCE: ("ITX_S3_BUCKET_REFERENCE", "itl-0004-itx-dev-poc-02-reference"),
        }
        env_var, default = mapping[layer]
        return os.environ.get(env_var, default)
    
    def _get_file_details(self, file_id: str, file_processing_date: str, ) -> dict[str, str]:
        """
        Recupera los metadatos del archivo (client_id, brand_id, file_type,
        file_processing_date, landing_file_name) desde DynamoDB, usados por
        `_get_s3_key_prefix`/`_get_s3_key` para construir la ruta S3 de cada
        capa.

        Args:
            file_id: PK de la tabla file_control.
            file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".

        Returns:
            dict con los campos de metadata del archivo.

        Ejemplo:
            fs._get_file_details("0A8221C3...", "2026-01-20")
        """
        return Database().get_ardef_file_control(
            file_id=file_id,
            file_processing_date=file_processing_date
        )
    
    def _get_s3_key_prefix(
        self, 
        layer: _Layer,
        file_id: str,
        file_processing_date: str, 
        subdir: str = "",
    ) -> str:
        """
        Construye el prefijo de S3 key (sin nombre de archivo) para una capa
        dada, a partir de los metadatos del archivo. LANDING usa solo
        `{client_id}/` (estructura plana); las demás capas usan
        `{client_id}/{brand_id}/{file_type}/{file_processing_date}/[{subdir}/]`.

        Args:
            layer: capa del pipeline.
            file_id: PK de la tabla file_control.
            file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".
            subdir: subcarpeta adicional dentro de la capa (ej. "400_ARDEF_CAL").

        Returns:
            Prefijo de key con "/" final, ej. "EBGR/VISA/IN/2026-01-20/400_ARDEF_CAL/".

        Ejemplo:
            fs._get_s3_key_prefix(FileStorage.Layer.STAGING, "0A8221C3...", "2026-01-20", "400_ARDEF_CAL")
        """
        details = self._get_file_details(
            file_id=file_id, 
            file_processing_date=file_processing_date
        )
 
        if layer == self.Layer.LANDING:
            return f"{details['client_id']}/"
        
        parts = [
            details["client_id"],
            details["brand_id"],
            details["file_type"],
            details["file_processing_date"],
        ]
        if subdir:
            parts.append(subdir)
 
        return "/".join(parts) + "/"
    
    def _get_s3_key(
            self,
            layer: _Layer,
            file_id: str,
            file_processing_date: str,
            subdir: str = "",
    ) -> str:
        """
        Construye la S3 key completa (con nombre de archivo) para una capa.
        LANDING usa el nombre de archivo original (`landing_file_name`); las
        demás capas usan el `file_id` como nombre base (sin extensión — los
        callers de lectura/escritura de parquet le agregan ".parquet").

        Args:
            layer: capa del pipeline.
            file_id: PK de la tabla file_control.
            file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".
            subdir: subcarpeta adicional dentro de la capa.

        Returns:
            S3 key completa, ej. "EBGR/VISA/IN/2026-01-20/400_ARDEF_CAL/0A8221C3...".

        Ejemplo:
            fs._get_s3_key(FileStorage.Layer.STAGING, "0A8221C3...", "2026-01-20", "400_ARDEF_CAL")
        """
        details = self._get_file_details(file_id=file_id, file_processing_date=file_processing_date)
        prefix = self._get_s3_key_prefix(layer, file_id, file_processing_date, subdir)
 
        if layer == self.Layer.LANDING:
            return prefix + details["landing_file_name"]
        
        return prefix + file_id
    
    def read_plaintext(
            self,
            layer: Layer,
            file_id: str,
            file_processing_date: str,
            subdir: str = "",
            encoding: str = "Latin-1",
    ) -> pd.DataFrame:
        """
        Lee el archivo fuente ARDEF (texto plano, encoding Latin-1 por
        default) desde S3 y lo retorna como un DataFrame de una sola columna
        ('lines'), una fila por línea no vacía del archivo. Si el objeto no
        existe en S3 (`ClientError`), retorna un DataFrame vacío en vez de
        propagar la excepción.

        Args:
            layer: capa S3 de origen (típicamente LANDING).
            file_id: PK de la tabla file_control.
            file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".
            subdir: subcarpeta adicional dentro de la capa.
            encoding: encoding del archivo fuente (default "Latin-1").

        Returns:
            DataFrame con columna 'lines' (dtype str), una fila por línea del
            archivo; vacío si el objeto S3 no existe.

        Ejemplo:
            fs.read_plaintext(FileStorage.Layer.LANDING, "0A8221C3...", "2026-01-20")
        """
        bucket = self._get_bucket(layer)
        key = self._get_s3_key(layer, file_id, file_processing_date, subdir)
 
        log.logger.debug(f"Leyendo texto: s3//{bucket}/{key}")
 
        try: 
            response = self._get_client().get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode(encoding)
        except ClientError as exc:
            log.logger.error(
                f"Error S3 [{exc.response['Error']['Code']}] | "
                f"s3://{bucket}/{key}"
            )
            return pd.DataFrame([], columns=["lines"], dtype=str)
        
        lines = [
            line.rstrip("\r\n")
            for line in content.split("\n")
            if line.rstrip("\r\n") != ""
        ]
 
        return pd.DataFrame(lines, columns=["lines"], dtype=str)
    
    def write_plaintext(self) -> None:
        raise NotImplementedError
    
    def read_parquet(
        self, 
        layer: Layer,
        file_id: str,
        file_processing_date: str,
        subdir: str = "",
    ) -> pd.DataFrame:
        """
        Lee un parquet de una etapa del pipeline ARDEF (staging u
        operational) desde S3, usando un buffer BytesIO en memoria (sin
        pasar por disco).

        Args:
            layer: capa S3 de origen.
            file_id: PK de la tabla file_control.
            file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".
            subdir: subcarpeta de la etapa (ej. "300_ARDEF_CLN").

        Returns:
            DataFrame con el contenido del parquet.

        Ejemplo:
            fs.read_parquet(FileStorage.Layer.STAGING, "0A8221C3...", "2026-01-20", "300_ARDEF_CLN")
        """
        bucket = self._get_bucket(layer)
        key = self._get_s3_key(layer, file_id, file_processing_date, subdir) + ".parquet"
 
        log.logger.debug(f"Leyendo parquet: s3://{bucket}/{key}")
 
        response = self._get_client().get_object(Bucket=bucket, Key=key)
 
        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer)
    
    def read_parquet_by_filepath(
        self, 
        filepath: str,
        layer: Layer = _Layer.REFERENCE,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Lee un parquet desde S3 usando una S3 key directa.
        Lanza FileNotFoundError si la key no existe (primera ejecución de lu_ardef).
 
        Args:
            filepath:   S3 key dentro del bucket de la capa indicada.
            layer:      capa S3 donde reside el archivo.
                        Default = REFERENCE (itl-0004-itx-dev-poc-02-reference),
                        que es donde vive lu_ardef.parquet
            columns:    lista de columnas a leer. None = todas las columnas.
                        Usar para cargar solo las columnas necesarias y reducir
                        el uso de memoria (e.g. columnas de lógica de calculate).

        Returns:
            DataFrame con el contenido del parquet (solo las columnas pedidas).

        Raises:
            FileNotFoundError: si la key no existe en S3.

        Ejemplo:
            fs.read_parquet_by_filepath("visa_ardef/data.parquet")
        """
        bucket = self._get_bucket(layer)
 
        log.logger.debug(f"Leyendo parquet por key: s3//{bucket}/{filepath}")
 
        try: 
            response = self._get_client().get_object(Bucket=bucket, Key=filepath)
            buffer = io.BytesIO(response["Body"].read())
            return pd.read_parquet(buffer, columns=columns)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"S3 key no encontrada: s3://{bucket}/{filepath}")
            raise
    
    def read_arrow_by_filepath(
        self,
        filepath: str,
        layer: Layer = _Layer.REFERENCE,
    ) -> pa.Table:
        """
        Lee un parquet desde S3 y lo retorna como PyArrow Table.
 
        Usar cuando se necesita cargar la tabla completa (todas las columnas) pero
        minimizando el uso de RAM. PyArrow almacena los datos en buffers contiguos,
        siendo 5-6x más eficiente en memoria que un DataFrame pandas con object dtype.
 
        Args:
            filepath:   S3 key dentro del bucket de la capa indicada.
            layer:      capa S3 donde reside el archivo. Default = REFERENCE.

        Returns:
            pa.Table con el contenido completo del parquet.

        Raises:
            FileNotFoundError: si la key no existe en S3.

        Ejemplo:
            fs.read_arrow_by_filepath("visa_ardef/data.parquet")
        """
        bucket = self._get_bucket(layer)
 
        log.logger.debug(f"Leyendo Arrow table por key: s3//{bucket}/{filepath}")
 
        try:
            response = self._get_client().get_object(Bucket=bucket, Key=filepath)
            buffer = io.BytesIO(response["Body"].read())
            return pq.read_table(buffer)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"S3 key no encontrada: s3://{bucket}/{filepath}")
            raise
 
    def write_parquet(
        self,
        data: pd.DataFrame,
        layer: Layer,
        file_id: str,
        file_processing_date: str,
        subdir: str = "",
        index: bool = False,
    ) -> str:
        """
        Serializa un DataFrame como parquet (buffer BytesIO en memoria, sin
        pasar por disco) y lo sube a la capa/subdir indicados. Retorna la S3
        URI del objeto creado.

        Args:
            data: DataFrame a serializar.
            layer: capa S3 de destino.
            file_id: PK de la tabla file_control.
            file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".
            subdir: subcarpeta de la etapa (ej. "500_ARDEF_OPE").
            index: incluir índice pandas en el parquet.

        Returns:
            S3 URI del objeto escrito, ej. "s3//bucket/EBGR/VISA/IN/2026-01-20/500_ARDEF_OPE/0A8221C3....parquet".

        Ejemplo:
            fs.write_parquet(df, FileStorage.Layer.OPERATIONAL, "0A8221C3...", "2026-01-20", "500_ARDEF_OPE")
        """
        bucket = self._get_bucket(layer)
        key = self._get_s3_key(layer, file_id, file_processing_date, subdir) + ".parquet"
 
        buffer = io.BytesIO()
        data.to_parquet(buffer, index=index)
        buffer.seek(0)
 
        log.logger.debug(f"Escribiendo parquet: s3//{bucket}/{key}")
 
        self._get_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
 
        return f"s3//{bucket}/{key}"
    
    def write_parquet_by_filepath(
        self,
        data: pd.DataFrame,
        filepath: str,
        index: bool = False,
        *,
        layer: Layer = _Layer.REFERENCE,
        schema: pa.Schema | None = None,
        compression: str = "snappy",
    ) -> None:
        """
        Sube un DataFrame pandas como parquet a S3 usando una S3 key directa.
 
        Args:
            data:           DataFrame a serializar.
            filepath:       S3 key dentro del bucket de la capa indicada.
            index:          incluir índice pandas en el parquet.
            layer:          capa S3 de destino. Default = REFERENCE.
            schema:         schema PyArrow opcional para forzar tipos en la escritura.
            compression:    algoritmo de compresión parquet (default: snappy).

        Returns:
            None. Sube el parquet a S3 como efecto secundario.

        Ejemplo:
            fs.write_parquet_by_filepath(df, "visa_ardef/data.parquet")
        """
        bucket = self._get_bucket(layer)
        buffer = io.BytesIO()
 
        if schema is None:
            data.to_parquet(buffer, index=index, compression=compression)
        else:
            present = set(data.columns)
            schema_filtered = pa.schema([f for f in schema if f.name in present])
            table = pa.Table.from_pandas(data, schema=schema_filtered, preserve_index=index)
            pq.write_table(table, buffer, compression=compression)
 
        buffer.seek(0)
 
        log.logger.debug(f"Escribiendo parquet por key: s3//{bucket}/{filepath}")
 
        self._get_client().put_object(
            Bucket=bucket, 
            Key=filepath,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
 
    def write_arrow_by_filepath(
        self,
        table: pa.Table,
        filepath: str,
        *,
        layer: Layer = _Layer.REFERENCE,
        compression: str = "snappy",
    ) -> None:
        """
        Sube un PyArrow Table como parquet a S3 usando una S3 key directa.
 
        Usar junto a read_arrow_by_filepath para operaciones sobre lu_ardef
        que requieren mantener el uso de RAM bajo (tablas de millones de filas).
 
        Args:
            table:          PyArrow Table a serializar.
            filepath:       S3 key dentro del bucket de la capa indicada.
            layer:          capa S3 de destino. Default = REFERENCE.
            compression:    algoritmo de compresión parquet (default: snappy).

        Returns:
            None. Sube el parquet a S3 como efecto secundario.

        Ejemplo:
            fs.write_arrow_by_filepath(table, "visa_ardef/data.parquet")
        """
        bucket = self._get_bucket(layer)
        buffer = io.BytesIO()
 
        pq.write_table(table, buffer, compression=compression)
        buffer.seek(0)
 
        log.logger.debug(f"Escribiendo Arrow table por key: s3//{bucket}/{filepath}")
 
        self._get_client().put_object(
            Bucket=bucket,
            Key=filepath,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
 
    def get_lu_ardef_filepath(
        self, 
        file_id: str = "",
        file_processing_date: str = "",
        filename: str = "lu_ardef.parquet",
    ) -> str:
        """
        Retorna la S3 key de la maestra ARDEF (lu_ardef) dentro del bucket
        REFERENCE. La ruta es fija e independiente del cliente o fecha de
        procesamiento — los parámetros existen solo por compatibilidad de
        firma con otros getters de key (`_get_s3_key`), pero no afectan el
        resultado.

        Bucket: itl-0004-itx-dev-poc-02-reference (Layer.REFERENCE)
        ARN: arn:aws:s3:::itl-0004-itx-dev-poc-02-reference

        Args:
            file_id: sin uso (mantenido por compatibilidad de firma).
            file_processing_date: sin uso (mantenido por compatibilidad de firma).
            filename: sin uso (mantenido por compatibilidad de firma).

        Returns:
            S3 key fija "visa_ardef/data.parquet".

        Ejemplo:
            fs.get_lu_ardef_filepath()  # "visa_ardef/data.parquet"
        """
        return "visa_ardef/data.parquet"
    
    def get_list_files_folderpath(
        self,
        layer: Layer,
        file_id: str,
        file_processing_date: str,
        subdir: str = "",
    ) -> list[str]:
        """
        Lista las S3 keys de parquets bajo el prefijo de la capa/subdir cuyo
        nombre de archivo empieza con `file_id`, para soportar el patrón de
        "listar solo los outputs de esta ejecución" (ver `decisions.md` —
        filtrado por file_id para no reprocesar ejecuciones anteriores). Si
        la operación de listado en S3 falla, retorna lista vacía en vez de
        propagar la excepción.

        Args:
            layer: capa S3 a listar.
            file_id: PK de la tabla file_control, usado como filtro de prefijo
                de nombre de archivo.
            file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".
            subdir: subcarpeta de la etapa.

        Returns:
            Lista ordenada de S3 keys (".parquet") cuyo nombre de archivo
            empieza con `file_id`; vacía si no hay resultados o si falla el
            listado.

        Ejemplo:
            fs.get_list_files_folderpath(FileStorage.Layer.STAGING, "0A8221C3...", "2026-01-20", "300_ARDEF_CLN")
        """
        bucket = self._get_bucket(layer)
        prefix = self._get_s3_key_prefix(layer, file_id, file_processing_date, subdir)
 
        try: 
            response = self._get_client().list_objects_v2(Bucket=bucket, Prefix=prefix)
        except ClientError as exc:
            log.logger.error(f"Error listando S3 s3://{bucket}/{prefix}: {exc}")
            return []
        
        return sorted([
            obj["Key"]
            for obj in response.get("Contents", []) 
            if obj["Key"].endswith(".parquet") 
            and obj["Key"].split("/")[-1].startswith(file_id)
        ])
