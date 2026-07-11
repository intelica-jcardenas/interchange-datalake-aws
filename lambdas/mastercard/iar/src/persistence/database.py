"""
database.py

Módulo interno del pipeline IAR (Mastercard) — capa de acceso a DynamoDB,
usado por `persistence/file.py` (`FileStorage`) para leer configuración de
cliente (tabla `client`) y metadata de archivo (tabla `file_control`).
Envoltorio delgado sobre boto3 que normaliza el resultado a un DataFrame de
pandas de una sola fila.
"""

import os
import boto3
import pandas as pd


class Database:
    """
    Cliente de solo lectura contra las tablas DynamoDB `client` y
    `file_control` del pipeline. Los nombres reales de las tablas se toman
    de variables de entorno (`DDB_CLIENT_TABLE`/`DDB_FILE_CONTROL_TABLE`),
    igual que en el resto de las Lambdas del proyecto (diseño
    configuration-driven, ver CLAUDE.md).
    """

    def __init__(self):
        """
        Inicializa el recurso de DynamoDB (boto3) y resuelve los nombres de
        tabla desde las variables de entorno `DDB_CLIENT_TABLE` y
        `DDB_FILE_CONTROL_TABLE`.

        Returns:
            None.

        Ejemplo:
            db = Database()
        """
        self.dynamodb = boto3.resource("dynamodb")
        self.client_table_name = os.environ["DDB_CLIENT_TABLE"]
        self.file_control_table_name = os.environ["DDB_FILE_CONTROL_TABLE"]

    def read_records(
        self,
        table_name: str,
        fields: list[str],
        where: dict = {},
    ) -> pd.DataFrame:
        """
        Lee un único item de DynamoDB (`get_item`, no `scan`/`query`) de la
        tabla `client` (llave `client_id`) o `file_control` (llave
        `file_id`), y lo proyecta a un DataFrame de una sola fila con
        únicamente las columnas pedidas en `fields`.

        Args:
            table_name: "client" o "file_control" (cualquier otro valor
                devuelve un DataFrame vacío con las columnas de `fields`).
            fields: Lista de nombres de atributo a extraer del item.
            where: dict con la llave primaria a buscar — `{"client_id": ...}`
                para "client", `{"file_id": ...}` para "file_control".

        Returns:
            DataFrame de 1 fila con `fields` como columnas, o un DataFrame
            vacío (mismas columnas, 0 filas) si la tabla no es reconocida o
            el item no existe.

        Ejemplo:
            db.read_records("client", ["file_iar_block", "file_iar_encoding"],
                where={"client_id": "SBSA"})
        """

        if table_name == "client":
            table = self.dynamodb.Table(self.client_table_name)

            response = table.get_item(
                Key={
                    "client_id": where["client_id"]
                }
            )

            item = response.get("Item")

        elif table_name in ("file_control"):
            table = self.dynamodb.Table(self.file_control_table_name)

            response = table.get_item(
                Key={
                    "file_id": where["file_id"]
                }
            )

            item = response.get("Item")

        else:
            return pd.DataFrame(columns=fields)

        if not item:
            return pd.DataFrame(columns=fields)

        return pd.DataFrame(
            [[item.get(field) for field in fields]],
            columns=fields
        )