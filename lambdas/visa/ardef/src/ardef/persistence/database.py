"""
database.py

Acceso a la tabla DynamoDB file_control para el motor de reglas ARDEF de
Visa. Usado por `ardef.persistence.file.FileStorage` para resolver, a partir
de un file_id, los metadatos (client_id, brand_id, file_type,
file_processing_date, landing_file_name) necesarios para construir las
S3 keys de cada capa del pipeline (landing/staging/operational/reference).
"""

import os
from datetime import date

import boto3
from botocore.exceptions import ClientError

from ardef.logs.logger import Logger

log = Logger(__name__)

class Database:
    """
    Encapsula el acceso de solo lectura a la tabla file_control en DynamoDB
    (PK: file_id), usada para recuperar los metadatos de un archivo ya
    registrado por el router.
    """

    DEFAULT_TABLE = 'itl-0004-itx-dev-dynamo-file_control-02'

    def __init__(self) -> None:
        """
        Inicializa el nombre de tabla y región desde variables de entorno
        (con defaults de desarrollo) y difiere la creación del recurso
        boto3 hasta el primer uso (ver `_get_resource`).

        Ejemplo:
            db = Database()  # table_name="itl-0004-itx-dev-dynamo-file_control-02"
        """
        self.table_name = os.environ.get("ITX_TABLE_FILE_CONTROL", self.DEFAULT_TABLE)
        self.region = os.environ.get("AWS_REGION", "eu-south-2")
        self._dynamodb = None

    def _get_resource(self):
        """
        Devuelve el recurso boto3 DynamoDB, creándolo perezosamente en la
        primera llamada y reutilizándolo en las siguientes (evita reabrir
        conexión en cada invocación dentro del mismo ciclo de vida Lambda).

        Returns:
            El recurso `boto3.resource("dynamodb", ...)` cacheado en la instancia.

        Ejemplo:
            resource = db._get_resource()
        """
        if self._dynamodb is None:
            self._dynamodb = boto3.resource("dynamodb", region_name=self.region)
        return self._dynamodb

    def get_ardef_file_control(
        self,
        file_id: str,
        file_processing_date: str,
        fields: list[str] | None = None,
    ) -> dict[str, str]:
        """
        Lee el registro de un archivo desde DynamoDB por file_id y valida que
        file_processing_date coincida con el registro encontrado, como
        chequeo de consistencia contra el parámetro recibido por el caller
        (evita usar el registro equivocado si el file_id fuera ambiguo).

        Args:
            file_id: PK de la tabla file_control (ej. "0A8221C3293EF535621FB1E35D709ACC").
            file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".
            fields: lista de campos a devolver del item. Si es None, usa el
                default (file_id, client_id, brand_id, file_type,
                file_processing_date, landing_file_name).

        Returns:
            dict[str, str] con los campos solicitados.

        Raises:
            ValueError: si el registro no existe o la fecha no coincide.
            ClientError: si hay error de comunicación con DynamoDB

        Ejemplo:
            Database().get_ardef_file_control("0A8221C3...", "2026-01-20")
            # {"file_id": "0A8221C3...", "client_id": "EBGR", ...}
        """
        if fields is None:
            fields = [
                "file_id",
                "client_id",
                "brand_id",
                "file_type",
                "file_processing_date",
                "landing_file_name",
            ]

        log.logger.debug(f"DynamoDB get_item | table={self.table_name} | file_id={file_id}")

        try:
            table = self._get_resource().Table(self.table_name)
            response = table.get_item(Key={"file_id": file_id})
        except ClientError as exc:
            log.logger.error(
                f"Error DynamoDB [{exc.response['Error']['Code']}] | "
                f"tabla={self.table_name} | file_id={file_id}"
            )
            raise

        if "Item" not in response:
            raise ValueError(
                f"No existe registro en file_control para "
                f"file_id={file_id}, file_processing_date={file_processing_date}"
            )
        
        item = response["Item"]

        store_date = _normalize_date(item.get("file_processing_date", ""))
        expected_date = str(file_processing_date).strip()

        if store_date != expected_date:
            raise ValueError(
                f"file_processing_date no coincide | "
                f"esperado={expected_date} | encontrado={store_date} | "
                f"file_id={file_id}"
            )
        
        log.logger.debug(
            f"Registro encontrado | file_id={file_id} | "
            f"client_id={item.get('client_id')} | brand_id={item.get('brand_id')}"
        )

        return {
            field: (
                _normalize_date(item[field])
                if field == "file_processing_date" and field in item
                else str(item.get(field, ""))
            )
            for field in fields
        }
    

def _normalize_date(value) -> str:
    """
    Convierte cualquier representación de fecha (objeto `date` de Python o
    string ya formateado) a un string plano "YYYY-MM-DD", para poder
    comparar el valor almacenado en DynamoDB contra el parámetro recibido
    sin importar cómo llegó serializado.

    Args:
        value: valor de fecha, `datetime.date` o cualquier otro tipo
            convertible a string (ej. Decimal/str desde DynamoDB).

    Returns:
        String "YYYY-MM-DD" (o `str(value).strip()` si no es un `date`).

    Ejemplo:
        _normalize_date(date(2026, 1, 20))  # "2026-01-20"
    """
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value).strip()

