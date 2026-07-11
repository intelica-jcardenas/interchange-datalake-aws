"""
interpreter.py

Primera etapa del pipeline ARDEF. Lee el archivo ARDEF original desde
LANDING como texto plano latin-1, identifica las líneas de detalle (rango de
BIN / regla, prefijo "VL") descartando líneas de control ("C****"), y
extrae del header ("AAACTRNG"/"AEPACRN") la versión y fecha más reciente del
archivo. El resultado es un DataFrame RAW (una fila por línea de detalle,
todo como texto sin parsear) que `transform.py` usa como entrada.
"""

import gc
from datetime import datetime

import pandas as pd
import pyarrow as pa

from ardef.logs.logger import Logger
from ardef.persistence.file import FileStorage

log = Logger(__name__)
fs = FileStorage()

def load_as_text(
    layer: FileStorage.Layer,
    file_id: str,
    file_processing_date: str,
    subdir= "",
    encoding: str = "Latin-1"
) -> pd.DataFrame:
    """
    Lee el archivo ARDEF fuente como texto plano (una fila por línea) desde
    el layer y subdir indicados, sin escribir ningún parquet — función
    auxiliar puramente de lectura, usada por `interpretate_ardef`.

    Args:
        layer: Layer de FileStorage desde donde leer (típicamente LANDING).
        file_id: ID único del archivo.
        file_processing_date: Fecha de negocio del archivo, "YYYY-MM-DD".
        subdir: Subdirectorio dentro del layer, vacío si el archivo está en
            la raíz del cliente (caso ARDEF en LANDING).
        encoding: Encoding del archivo fuente (default "Latin-1", el usado
            por los archivos ARDEF de Visa).

    Returns:
        DataFrame con una columna ("lines") y una fila por línea de texto del
        archivo original.

    Ejemplo:
        load_as_text(FileStorage.Layer.LANDING, "ABC123", "2026-04-24")
    """

    return fs.read_plaintext(
        layer=layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=subdir,
        encoding=encoding,
    )
    
def _build_ardef_raw_dataframe(
        records: pd.DataFrame,
        file_id: str,
        file_processing_date: str,
) -> pd.DataFrame:
    """
    Convierte las líneas de texto del archivo ARDEF en el DataFrame RAW del
    pipeline: filtra solo las líneas de detalle (prefijo "VL", excluyendo
    líneas de control "C****") y extrae del header ("AAACTRNG"/"AEPACRN") la
    versión y fecha más reciente del archivo (si hay múltiples versiones en
    el header, se queda con la de número más alto). Agrega columnas de
    metadata (file_id, file_processing_date, line_no, timestamps) a cada
    línea de detalle conservada.

    Args:
        records: DataFrame con una columna "lines" (una fila por línea de
            texto del archivo original), tal como lo devuelve `load_as_text`.
        file_id: ID único del archivo.
        file_processing_date: Fecha de negocio del archivo, "YYYY-MM-DD".

    Returns:
        DataFrame RAW con columnas file_id, file_processing_date,
        ardef_version, ardef_header_date, line_no, lines,
        row_creation_timestamp, _eff_ts — todas como texto. Vacío (mismas
        columnas, 0 filas) si `records` no tenía líneas de detalle.

    Ejemplo:
        _build_ardef_raw_dataframe(records, "ABC123", "2026-04-24")
    """

    # Timestamp de creación del parquet (mismo instante para row_creation_timestamp y _eff_ts)
    _now = datetime.now()
    row_creation_timestamp = _now.strftime("%Y-%m-%d %H:%M:%S.") + \
        f"{_now.microsecond // 1000:03d}"
    eff_ts = row_creation_timestamp   # getdate() — momento exacto de ejecución de la fila
 
    if records.empty:
        log.logger.warning(
            f"No records found for file_id={file_id}, "
            f"file_processing_data={file_processing_date}"
        )
 
        return pd.DataFrame(
            [],
            columns=[
                "file_id",
                "file_processing_date",
                "ardef_version",
                "ardef_header_date",
                "line_no",
                "lines",
                "row_creation_timestamp",
                "_eff_ts",
            ],
            dtype=str,
        )
    
    lines: list[str] = []
    versions: list[tuple[str, str]] = []
 
    for record in records["lines"].astype(str):
        record = record.rstrip()
 
        if record[0:2] == "VL" and "C****" not in record:
            lines.append(record)
 
        if record[0:8] == "AAACTRNG" and record[10:17] == "AEPACRN":
            header_date = record[23:31]
            version_number = record[63:67]
            versions.append((version_number, header_date))
 
    ultimate_version = None
    ultimate_date = None
    date_formated_as = None
 
    if versions:
        ultimate_version, ultimate_date = max(
            versions,
            key=lambda x: int(x[0]) if str(x[0]).isdigit() else -1,
        )
 
        date_formated_as = (
            datetime.strptime(str(ultimate_date), "%Y%m%d")
            .date()
            .strftime("%Y-%m-%d")
        )
 
        destiny_file = (
            datetime.strptime(str(ultimate_date), "%Y%m%d")
            .date()
            .strftime("%y%m%d")
        )
 
        date_for_name = datetime.strptime(destiny_file, "%y%m%d").strftime("%Y%m%d")
 
        log.logger.info(
            f"ARDEF header detected"
            f"ultimate_version={ultimate_version}, "
            f"ultimate_date={ultimate_date}, "
            f"date_formated_as={date_formated_as}, "
            f"destiny_file={destiny_file}, "
            f"date_for_name={date_for_name}"
        )
 
    else:
        log.logger.warning(
            f"No ARDEF header found for file_id={file_id}, "
            f"file_processing_date={file_processing_date}"
        )
 
    df = pd.DataFrame(
        {
            "file_id": file_id,
            "file_processing_date": file_processing_date,
            "ardef_version": ultimate_version,
            "ardef_header_date": date_formated_as,
            "line_no": range(1, len(lines) + 1),
            "lines": lines,
            "row_creation_timestamp": row_creation_timestamp,
            "_eff_ts": eff_ts,
        }
    )
 
    return df.astype(str)

def interpretate_ardef(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        file_id: str,
        file_processing_date: str,
        origin_subdir: str = "",
        target_subdir: str = "100_ARDEF_RAW",
        encoding: str = "Latin-1",
) -> None:
    """
    Etapa INTERPRETER del pipeline ARDEF (primera etapa, exclusiva de este
    tipo de archivo). Lee el archivo original desde LANDING como texto,
    aísla las líneas de detalle y el header más reciente, y escribe el
    resultado como parquet RAW en STAGING.

    Pasos:
        1. Leer LANDING / {archivo original} como texto (`load_as_text`).
        2. Construir el DataFrame RAW (`_build_ardef_raw_dataframe`).
        3. Escribir STAGING / {brand_id} / {file_type} / {date} /
           100_ARDEF_RAW / {file_id}.parquet

    Args:
        origin_layer: Layer de origen (típicamente LANDING).
        target_layer: Layer de destino (típicamente STAGING).
        file_id: ID único del archivo.
        file_processing_date: Fecha de negocio del archivo, "YYYY-MM-DD".
        origin_subdir: Subdirectorio de origen (vacío, el archivo ARDEF está
            en la raíz del cliente en LANDING).
        target_subdir: Subdirectorio de destino en STAGING (default
            "100_ARDEF_RAW").
        encoding: Encoding del archivo fuente (default "Latin-1").

    Returns:
        None. Escribe el parquet RAW en STAGING como efecto secundario y
        libera `records`/`df_raw` de memoria antes de retornar.

    Ejemplo:
        interpretate_ardef(FileStorage.Layer.LANDING, FileStorage.Layer.STAGING,
                            "ABC123", "2026-04-24")
    """

    records = load_as_text(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir,
        encoding=encoding,
    )
 
    df_raw = _build_ardef_raw_dataframe(
        records=records,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )
 
    output_filepath = fs.write_parquet(
        data=df_raw,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False,
    )
 
    log.logger.info(
        f"ARDEF RAW parquet created successfully: {output_filepath}"
    )
 
    del records
    del df_raw