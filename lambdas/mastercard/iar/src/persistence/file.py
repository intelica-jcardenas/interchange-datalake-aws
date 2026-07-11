"""
file.py

Módulo interno del pipeline IAR (Mastercard) — capa de acceso a S3, usado
por `handler.py` (`pipeline_iar`) para leer el archivo IAR de landing y
leer/escribir Parquets en staging, operational y reference. Encapsula la
convención de paths por capa (`_build_key`) y resuelve la fecha de
procesamiento y el nombre del archivo original consultando `file_control`
en DynamoDB (vía `persistence/database.py`).
"""

from enum import StrEnum, auto
import os
import io
from typing import Any

import boto3
import pandas as pd

from persistence.database import Database


class _Layer(StrEnum):
    """
    Capas del data lake que maneja este módulo, expuestas como
    `FileStorage.Layer`. Se usa `RAW` únicamente como valor del enum (el
    pipeline IAR no escribe una capa RAW separada en S3 — la capa RAW vive
    en memoria como los tres DataFrames de `raw.py`).
    """
    LANDING = auto()
    RAW = auto()
    STAGING = auto()
    OPERATIONAL = auto()
    REFERENCE = auto()

class FileStorage:
    """
    Fachada de acceso a S3 + DynamoDB para el pipeline IAR. Resuelve el
    bucket y la key S3 correctos según la capa (`Layer`) y el cliente/archivo,
    y expone lectura/escritura de bytes crudos y Parquet.
    """

    Layer = _Layer

    def __init__(self) -> None:
        """
        Inicializa el cliente S3 (boto3).

        Returns:
            None.

        Ejemplo:
            fs = FileStorage()
        """
        self.s3 = boto3.client("s3")

    def _get_file_details(self, client_id: str, file_id: str):
        """
        Consulta `file_control` (DynamoDB) para obtener la fecha de
        procesamiento y el nombre del archivo tal como llegó a landing —
        datos necesarios para construir las keys S3 de cualquier capa.

        Args:
            client_id: Identificador del cliente.
            file_id: Identificador del archivo en `file_control`.

        Returns:
            Fila (`pd.Series`) con `file_processing_date` y
            `landing_file_name`.

        Raises:
            ValueError: si `file_id` no existe en `file_control`.

        Ejemplo:
            self._get_file_details("SBSA", "ABC123...")
        """
        db = Database()

        df = db.read_records(
            table_name="file_control",
            fields=[
                "file_processing_date",
                "landing_file_name",
            ],
            where={
                "client_id": client_id,
                "file_id": file_id,
            },
        )

        if df.empty:
            raise ValueError(f"No se encontró file_id={file_id}")

        return df.iloc[0]

    def _get_bucket_by_layer(self, layer: _Layer) -> str:
        """
        Resuelve el nombre real del bucket S3 correspondiente a una capa,
        leyendo la variable de entorno asociada.

        Args:
            layer: Capa del data lake (`Layer.LANDING`/`STAGING`/
                `OPERATIONAL`/`REFERENCE`).

        Returns:
            Nombre del bucket S3.

        Raises:
            ValueError: si `layer` no tiene bucket configurado (ej.
                `Layer.RAW`, que no se usa en S3).

        Ejemplo:
            self._get_bucket_by_layer(Layer.STAGING)
            # -> "itl-0004-itx-dev-intchg-02-s3-staging"
        """
        if layer == self.Layer.LANDING:
            return os.environ["S3_LANDING_BUCKET"]

        if layer == self.Layer.STAGING:
            return os.environ["S3_STAGING_BUCKET"]

        if layer == self.Layer.OPERATIONAL:
            return os.environ["S3_OPERATIONAL_BUCKET"]

        if layer == self.Layer.REFERENCE:
            return os.environ["S3_REFERENCE_BUCKET"]

        raise ValueError(f"No existe bucket configurado para layer={layer}")

    def _build_key(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        filename: str | None = None,
    ) -> str:
        """
        Construye la key S3 según la capa: `LANDING` usa el nombre original
        del archivo (`{client_id}/{landing_file_name}`); `REFERENCE` usa un
        path fijo sin partición por fecha (`{subdir}/{filename}`, ej. la
        tabla maestra `mastercard_iar/data.parquet`); `STAGING`/`OPERATIONAL`
        particionan por fecha de procesamiento y por sub-etapa (`subdir`,
        ej. "RAW/header", "TRA", "CLN").

        Args:
            layer: Capa del data lake.
            client_id: Identificador del cliente.
            file_id: Identificador del archivo en `file_control`.
            subdir: Sub-ruta dentro de la capa (etapa del pipeline, o
                carpeta de referencia para `REFERENCE`).
            filename: Nombre del archivo Parquet a leer/escribir (requerido
                salvo para `LANDING`).

        Returns:
            Key S3 completa (sin el bucket).

        Raises:
            ValueError: si `filename` es requerido y no se especifica.

        Ejemplo:
            self._build_key(Layer.STAGING, "SBSA", "ABC123...", subdir="CLN",
                filename="ABC123....parquet")
        """
        file_details = self._get_file_details(client_id, file_id)

        processing_date = str(file_details["file_processing_date"])
        landing_file_name = file_details["landing_file_name"]

        if layer == self.Layer.LANDING:
            return f"{client_id}/{landing_file_name}"

        if layer == self.Layer.REFERENCE:
            return f"{subdir}/{filename}"

        if layer == self.Layer.OPERATIONAL:
            return f"{client_id}/MC/IAR/"f"date={processing_date}/{filename}"

        if not filename:
            raise ValueError("filename es obligatorio para STAGING/OPERATIONAL")

        return f"{client_id}/MC/IAR/"f"date={processing_date}/"f"process={subdir}/{filename}"

    def get_client_details(
        self,
        client_id: str,
        fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Consulta la configuración del cliente en DynamoDB (tabla `client`) —
        por defecto, si viene bloqueado (`file_iar_block`) y el encoding del
        archivo IAR (`file_iar_encoding`). El bloque comentado más abajo
        (`# TEMPORAL...`) es un fallback manual para desarrollo local sin
        permisos a la tabla `client`, no está activo en el flujo real.

        Args:
            client_id: Identificador del cliente.
            fields: Lista de atributos a leer; por defecto
                `["file_iar_block", "file_iar_encoding"]`.

        Returns:
            dict `{campo: valor}` con los campos pedidos.

        Ejemplo:
            fs.get_client_details("SBSA")
            # -> {"file_iar_block": True, "file_iar_encoding": "Latin-1"}
        """

        if fields is None:
            fields = [
                "file_iar_block",
                "file_iar_encoding",
            ]

        # TEMPORAL si no tienes permiso a tabla client:
        # return {
        #     "file_iar_block": True,
        #     "file_iar_encoding": "Latin-1",
        # }

       
        db = Database()
        row = db.read_records(
            table_name="client",
            fields=fields,
            where={"client_id": client_id},
        ).iloc[0]
        
        return {field: row.loc[field] for field in fields}

    def get_landing_object(
        self,
        client_id: str,
        file_id: str,
    ) -> tuple[str, str]:
        """
        Resuelve el bucket y la key S3 del archivo original en landing,
        usados solo para loguear la ruta de origen en `handler.py` (la
        lectura real de bytes se hace vía `read_binary`).

        Args:
            client_id: Identificador del cliente.
            file_id: Identificador del archivo en `file_control`.

        Returns:
            Tupla `(bucket, key)`.

        Ejemplo:
            fs.get_landing_object("SBSA", "ABC123...")
            # -> ("itl-0004-itx-dev-intchg-02-s3-landing", "SBSA/archivo.iar")
        """

        bucket = self._get_bucket_by_layer(self.Layer.LANDING)

        key = self._build_key(
            layer=self.Layer.LANDING,
            client_id=client_id,
            file_id=file_id,
        )

        return bucket, key

    def read_binary(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
    ) -> bytes:
        """
        Descarga el contenido completo (bytes crudos, sin parsear) de un
        archivo desde S3, usado en `handler.py` para leer el archivo IAR
        original de landing.

        Args:
            layer: Capa del data lake (normalmente `Layer.LANDING`).
            client_id: Identificador del cliente.
            file_id: Identificador del archivo en `file_control`.

        Returns:
            Contenido completo del archivo como `bytes`.

        Ejemplo:
            fs.read_binary(Layer.LANDING, "SBSA", "ABC123...")
        """

        bucket = self._get_bucket_by_layer(layer)

        key = self._build_key(
            layer=layer,
            client_id=client_id,
            file_id=file_id,
        )

        response = self.s3.get_object(
            Bucket=bucket,
            Key=key,
        )

        return response["Body"].read()

    def write_parquet(
        self,
        df: pd.DataFrame,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        filename: str = "data.parquet",
    ) -> str:
        """
        Serializa un DataFrame a Parquet (engine PyArrow, sin índice) en
        memoria y lo sube a S3 en la capa/ruta indicada.

        Args:
            df: DataFrame a escribir.
            layer: Capa del data lake destino.
            client_id: Identificador del cliente.
            file_id: Identificador del archivo en `file_control`.
            subdir: Sub-ruta dentro de la capa (ver `_build_key`).
            filename: Nombre del archivo Parquet.

        Returns:
            URI `s3://bucket/key` del archivo escrito.

        Ejemplo:
            fs.write_parquet(df_clean, Layer.STAGING, "SBSA", "ABC123...",
                subdir="CLN", filename="ABC123....parquet")
        """

        bucket = self._get_bucket_by_layer(layer)

        key = self._build_key(
            layer=layer,
            client_id=client_id,
            file_id=file_id,
            subdir=subdir,
            filename=filename,
        )

        buffer = io.BytesIO()

        df.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
        )

        buffer.seek(0)

        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
        )

        return f"s3://{bucket}/{key}"

    def read_parquet(
        self,
        layer: _Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        filename: str = "data.parquet",
    ) -> pd.DataFrame:
        """
        Descarga y deserializa un Parquet desde S3 a un DataFrame. Usado en
        `handler.py` para leer el histórico maestro existente en
        `s3-reference/mastercard_iar/data.parquet` antes de concatenarlo con
        los registros nuevos de la ejecución actual (si no existe, el
        caller captura la excepción de `get_object` y arranca el histórico
        desde cero).

        Args:
            layer: Capa del data lake origen.
            client_id: Identificador del cliente.
            file_id: Identificador del archivo en `file_control`.
            subdir: Sub-ruta dentro de la capa (ver `_build_key`).
            filename: Nombre del archivo Parquet.

        Returns:
            DataFrame con el contenido del Parquet leído.

        Ejemplo:
            fs.read_parquet(Layer.REFERENCE, "SBSA", "ABC123...",
                subdir="mastercard_iar", filename="data.parquet")
        """

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