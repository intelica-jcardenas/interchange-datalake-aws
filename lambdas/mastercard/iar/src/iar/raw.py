"""
raw.py

Capa RAW del pipeline IAR (Mastercard) — módulo interno importado por
`handler.py` (`pipeline_iar`, paso "RAW"). Lee el stream ya desbloqueado
(salida de `extract.py`) registro por registro usando un prefijo de longitud
de 4 bytes big-endian (análogo al RDW del flujo transaccional IPM), y separa
los records en tres capas: el header del archivo, el catálogo de tablas IPM
(IP0000T1, que declara qué tablas de negocio contiene el archivo y en qué
posición) y los records de detalle de cada tabla — sin interpretar todavía
las columnas de negocio (eso lo hace `transform.py`).
"""

import io
import struct
from logs.logger import logger
from datetime import datetime

import pandas as pd


def read_record_with_metadata(
    stream: io.BytesIO,
    encoding: str,
    record_sequence: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Lee un único record del stream IAR: primero 4 bytes big-endian con la
    longitud del record (prefijo tipo RDW), luego esa cantidad de bytes de
    payload, decodificados con `encoding`.

    Args:
        stream: Buffer posicionado al inicio de un record.
        encoding: Encoding de texto del archivo ("cp500" para EBCDIC o
            "Latin-1").
        record_sequence: Número secuencial de este record dentro del
            archivo, usado como metadata de trazabilidad.

    Returns:
        dict con `record_sequence`, `record_length` y `record_raw` (texto
        decodificado), o `None` si el stream ya terminó (menos de 4 bytes
        disponibles) o el largo declarado es 0.

    Ejemplo:
        read_record_with_metadata(stream, "Latin-1", 1)
        # -> {"record_sequence": 1, "record_length": 27, "record_raw": "..."}
    """
    raw_len = stream.read(4)

    if len(raw_len) < 4:
        return None

    record_length = struct.unpack(">i", raw_len)[0]

    if record_length == 0:
        return None

    raw_record = stream.read(record_length)

    if len(raw_record) < record_length:
        raise ValueError(
            f"Registro incompleto. Esperado={record_length}, leído={len(raw_record)}"
        )

    record_text = raw_record.decode(encoding)

    return {
        "record_sequence": record_sequence,
        "record_length": record_length,
        "record_raw": record_text,
    }


def parse_header_raw(record: dict) -> dict:
    """
    Interpreta el primer record del archivo IAR (header) según su longitud,
    que determina cuál de los dos layouts conocidos aplica: 27 caracteres
    (formato "update_header": fecha `%Y%m%d`) o 80 caracteres (formato
    "replace_header": fecha `%m%d%y` con separadores `/`). Ambos layouts
    también están descritos en `getIPMParameters()` (`transform.py`).

    Args:
        record: dict producido por `read_record_with_metadata` para el
            primer record del archivo.

    Returns:
        `record` extendido con `header_type`, `header_date`, `header_time`,
        `app_processing_date` (normalizada a `YYYYMMDD`) y
        `record_type="HEADER"`.

    Ejemplo:
        parse_header_raw({"record_raw": "..." , ...})
        # -> {..., "app_processing_date": "20260103", "record_type": "HEADER"}
    """
    record_raw = record["record_raw"]

    if len(record_raw) == 27:
        header_type = record_raw[0:15].strip()
        header_date = record_raw[15:23].strip()
        header_time = record_raw[23:28].strip()

        processing_date = datetime.strptime(header_date, "%Y%m%d").strftime("%Y%m%d")

    elif len(record_raw) == 80:
        header_type = record_raw[0:17].strip()
        header_date = record_raw[45:54].replace("/", "").strip()
        header_time = record_raw[61:69].strip()

        processing_date = datetime.strptime(header_date, "%m%d%y").strftime("%Y%m%d")

    else:
        raise ValueError(f"Header desconocido. Longitud detectada: {len(record_raw)}")

    return {
        **record,
        "header_type": header_type,
        "header_date": header_date,
        "header_time": header_time,
        "app_processing_date": processing_date,
        "record_type": "HEADER",
    }


def extract_raw_layers(
    stream: io.BytesIO,
    source_file: str,
    ebcdic: bool = True,
    encoding: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Genera la capa RAW del archivo IAR completo: lee el header, luego el
    catálogo de tablas IPM (IP0000T1, hasta encontrar su trailer) y
    finalmente todos los records de detalle, usando `read_record_with_metadata`
    para cada uno. El catálogo determina, vía `table_ipm_id`/`table_sub_id`,
    qué tabla de negocio (ej. IP0040T1) corresponde a cada grupo de records
    de detalle — información que `transform.py` usa para filtrar los
    records de la tabla que se está procesando. No parsea todavía las
    columnas de negocio de cada tabla.

    Args:
        stream: Buffer con el archivo IAR ya desbloqueado (salida de
            `extract.py`), posicionado al inicio.
        source_file: Ruta S3 del archivo origen (ej.
            "s3://bucket/SBSA/archivo.iar"), para trazabilidad en cada fila.
        ebcdic: Usado solo si `encoding` es `None`, para decidir entre
            "cp500" (EBCDIC) y "Latin-1".
        encoding: Encoding explícito a usar; si se pasa, tiene prioridad
            sobre `ebcdic`.

    Returns:
        Tupla `(df_header, df_catalog, df_records)`: header (1 fila),
        catálogo de tablas IPM detectadas (+ trailer), y records de detalle
        sin parsear.

    Ejemplo:
        extract_raw_layers(stream, "s3://bucket/SBSA/archivo.iar", encoding="Latin-1")
    """

    if encoding is None:
        encoding = "cp500" if ebcdic else "Latin-1"

    header_records = []
    catalog_records = []
    detail_records = []

    record_sequence = 1

    # HEADER
    header = read_record_with_metadata(
        stream=stream,
        encoding=encoding,
        record_sequence=record_sequence,
    )

    if header is None:
        raise ValueError("Archivo vacío o sin header")

    header_raw = parse_header_raw(header)
    header_records.append(
        {
            **header_raw,
            "source_file": source_file,
        }
    )

    processing_date = header_raw["app_processing_date"]

    record_sequence += 1

    # CATALOG IP0000T1
    while True:
        record = read_record_with_metadata(
            stream=stream,
            encoding=encoding,
            record_sequence=record_sequence,
        )

        if record is None:
            break

        record_raw = record["record_raw"]

        key = record_raw[11:19]

        if key != "IP0000T1":
            if record_raw.startswith("TRAILER RECORD IP0000T1"):
                catalog_records.append(
                    {
                        **record,
                        "source_file": source_file,
                        "app_processing_date": processing_date,
                        "record_type": "CATALOG_TRAILER",
                    }
                )
                record_sequence += 1
                break

            raise ValueError("No se encontró trailer de IP0000T1")

        table_ipm_id = record_raw[19:27]
        table_sub_id = record_raw[243:246]

        catalog_records.append(
            {
                **record,
                "source_file": source_file,
                "app_processing_date": processing_date,
                "record_type": "CATALOG",
                "table_ipm_id": table_ipm_id,
                "table_sub_id": table_sub_id,
            }
        )

        record_sequence += 1

    # DETAIL RECORDS
    while True:
        record = read_record_with_metadata(
            stream=stream,
            encoding=encoding,
            record_sequence=record_sequence,
        )

        if record is None:
            break

        record_raw = record["record_raw"]

        if record_raw.startswith("TRAILER RECORD"):

            #record_type = "TRAILER"
            record_table_id = None

        else:

            #record_type = "DETAIL"
            record_table_id = (
                record_raw[8:11]
                if len(record_raw) >= 11
                else None
            )

        detail_records.append(
            {
                **record,
                "source_file": source_file,
                "app_processing_date": processing_date,
                "record_type": "DETAIL",
                "record_table_id": record_table_id,
            }
        )

        record_sequence += 1

    df_catalog_tmp = pd.DataFrame(catalog_records)
    
    tables_detected = (
        df_catalog_tmp["table_ipm_id"]
        .dropna()
        .unique()
        .tolist()
        if not df_catalog_tmp.empty and "table_ipm_id" in df_catalog_tmp.columns
        else []
    )

    logger.info(
        "Tablas detectadas en el archivo IAR: "
        f"{', '.join(tables_detected)}"
    )

    logger.info(
        f"Total tablas detectadas: {len(tables_detected)}"
    )
    
    return (
        pd.DataFrame(header_records),
        pd.DataFrame(catalog_records),
        pd.DataFrame(detail_records),
    )

    