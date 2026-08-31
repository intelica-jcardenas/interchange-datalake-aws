"""
handler.py — Lambda real: itl-0004-itx-{env}-intchg-02-lmbd-vi-extract
================================================================================
Archivo:     lambdas/visa/extract/src/handler.py

Tercera etapa del pipeline Visa (tras transform, antes de clean). Extrae
campo por campo de cada record de ancho fijo generado por transform,
usando las posiciones y longitudes definidas en la tabla DynamoDB
`visa_fields` (GSI `type-record-index`). Soporta records simples (BASEII,
VSS) y records con sub-identificador secundario (SMS, donde varios campos
comparten la misma columna `tcsn` pero se distinguen por un
`secondary_identifier` en una posición fija dentro del record). Procesa el
Parquet de transform en streaming (`iter_batches` + `ParquetWriter`) y
escribe el resultado a `s3-staging/200_*_ext_*/`.

Optimizado (v2) respecto a una versión anterior más lenta: usa
`itertuples()` en vez de `iterrows()` para recorrer las definiciones de
campo (acceso por atributo en vez de lookup por dict, ~10x más rápido), y
arma cada chunk extraído con un único `pd.concat()` de Series en vez de
ensamblar un dict campo por campo.

Flujo:
1. Por cada output de transform (uno por type_record): cargar definiciones
   de campo desde DynamoDB (ordenadas por sort_by de OUTPUT_TYPE_CONFIG)
2. Si es SMS: aplicar preprocesamiento especial de field_defs
3. Descargar el Parquet de transform a un buffer en memoria
4. Iterar el Parquet en chunks (EXTRACT_CHUNK_SIZE, default 300000 filas)
5. Por chunk: agrupar campos por columna tcsn/secondary_identifier y
   extraer cada campo por posición/longitud
6. Insertar content_hash como primera columna
7. Escribir cada chunk al ParquetWriter en streaming
8. Subir el Parquet consolidado a s3-staging

Variables de entorno:
  S3_BUCKET_STAGING          : bucket con los Parquets de transform, y destino del extract
  DYNAMODB_FIELD_DEFINITION  : tabla de definición de campos Visa (default: itx-visa-fields)
  EXTRACT_CHUNK_SIZE         : filas por chunk de streaming (default: 300000)
"""

import os
import json
import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from typing import Optional, Dict, List
from boto3.dynamodb.conditions import Key
import time

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3       = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

STAGING_BUCKET  = os.environ.get('S3_BUCKET_STAGING')
FIELD_DEF_TABLE = os.environ.get('DYNAMODB_FIELD_DEFINITION', 'itx-visa-fields')
CHUNK_SIZE      = int(os.environ.get('EXTRACT_CHUNK_SIZE', '300000'))

# =============================================================================
# MAPEO DE CONFIGURACIÓN POR TIPO DE OUTPUT
# =============================================================================

OUTPUT_TYPE_CONFIG = {
    "BASEII": {
        "type_record": "draft",
        "input_subdir": "100_baseii_raw_drafts",
        "output_subdir": "200_baseii_ext_drafts",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
    "SMS": {
        "type_record": "sms",
        "input_subdir": "100_sms_raw_messages",
        "output_subdir": "200_sms_ext_messages",
        "sort_by": ["secondary_identifier", "position"],
        "special_processing": "sms"
    },
    "VSS_110": {
        "type_record": "vss_110",
        "input_subdir": "100_vss_110_raw",
        "output_subdir": "200_vss_110_ext",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
    "VSS_120": {
        "type_record": "vss_120",
        "input_subdir": "100_vss_120_raw",
        "output_subdir": "200_vss_120_ext",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
    "VSS_130": {
        "type_record": "vss_130",
        "input_subdir": "100_vss_130_raw",
        "output_subdir": "200_vss_130_ext",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
    "VSS_140": {
        "type_record": "vss_140",
        "input_subdir": "100_vss_140_raw",
        "output_subdir": "200_vss_140_ext",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
}

# =============================================================================
# FUNCIONES DE ACCESO A DATOS
# =============================================================================

def _load_field_definitions(type_record: str, sort_by: List[str]) -> pd.DataFrame:
    """
    Consulta en DynamoDB (tabla `visa_fields`, GSI `type-record-index`)
    todas las definiciones de campo para un `type_record` dado, paginando
    con `LastEvaluatedKey`, y las ordena según `sort_by` para garantizar un
    orden determinístico de extracción.

    Args:
        type_record: Tipo de record Visa a consultar, ej. "draft" para
            BASEII.
        sort_by: Lista de columnas por las que ordenar el resultado (de
            `OUTPUT_TYPE_CONFIG[...]['sort_by']`), ej.
            `["tcsn", "position", "secondary_identifier_len"]`.

    Returns:
        DataFrame con una fila por campo definido, ordenado por
        `sort_by`, o un DataFrame vacío si no hay definiciones para ese
        `type_record`.

    Ejemplo:
        _load_field_definitions("draft", ["tcsn", "position"])
    """
    logger.info(f"Loading field definitions for type_record: {type_record}")

    table    = dynamodb.Table(FIELD_DEF_TABLE)
    response = table.query(
        IndexName='type-record-index',
        KeyConditionExpression=Key('type_record').eq(type_record)
    )
    items = response.get('Items', [])

    while 'LastEvaluatedKey' in response:
        response = table.query(
            IndexName='type-record-index',
            KeyConditionExpression=Key('type_record').eq(type_record),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))

    if not items:
        logger.warning(f"No field definitions found for type_record: {type_record}")
        return pd.DataFrame()

    df = pd.DataFrame(items)

    int_cols = [
        'position', 'length', 'secondary_identifier_pos',
        'secondary_identifier_len', 'sort_order'
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    sort_cols = [c for c in sort_by if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=True)

    logger.info(f"Loaded {len(df)} field definitions for {type_record}")
    return df


def _get_s3_file_object(s3_key: str) -> BytesIO:
    """
    Descarga un objeto de `S3_BUCKET_STAGING` completo a un buffer en
    memoria.

    Args:
        s3_key: Key del objeto dentro del bucket de staging.

    Returns:
        Buffer `BytesIO` con el contenido del objeto. Relanza cualquier
        excepción tras loguearla.

    Ejemplo:
        _get_s3_file_object("EBGR/VISA/100_baseii_raw_drafts/.../x.parquet")
    """
    logger.info(f"Downloading file into memory buffer from s3://{STAGING_BUCKET}/{s3_key}")
    try:
        response = s3.get_object(Bucket=STAGING_BUCKET, Key=s3_key)
        return BytesIO(response['Body'].read())
    except Exception as e:
        logger.error(f"Error reading Parquet {s3_key}: {str(e)}")
        raise


def _process_sms_field_defs(field_defs: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta las definiciones de campo SMS antes de la extracción: descarta
    el `secondary_identifier` "V22000" (no corresponde a ningún campo real
    extraíble) y le quita el prefijo "V" al resto (ej. "V22200" → "22200"),
    para que coincida con el nombre de columna real que usa `transform`
    para esa columna secundaria.

    Args:
        field_defs: DataFrame de definiciones de campo SMS ya cargado de
            DynamoDB.

    Returns:
        DataFrame ajustado, o el mismo `field_defs` sin cambios si no
        tiene columna `secondary_identifier`.

    Ejemplo:
        _process_sms_field_defs(field_defs)  # 'V22200' -> '22200' en la columna
    """
    if 'secondary_identifier' in field_defs.columns:
        field_defs = field_defs[
            field_defs['secondary_identifier'] != 'V22000'
        ].copy()
        field_defs['secondary_identifier'] = field_defs['secondary_identifier'].apply(
            lambda x: str(x)[1:] if x and str(x).startswith('V') else x
        )
    return field_defs


# =============================================================================
# LÓGICA DE EXTRACCIÓN DE CAMPOS
# MEJORA 1: itertuples() en vez de iterrows()
# MEJORA 2: dict → DataFrame en vez de concat de 250 Series
# =============================================================================

def _extract_fields(data, field_defs, type_record):
    """
    Extrae todos los campos definidos en `field_defs` de un chunk de
    records de ancho fijo (`data`), devolviendo un DataFrame con una
    columna por campo extraído.

    Agrupa las definiciones de campo por la columna origen (`tcsn` si
    existe como columna en `data`, sino `secondary_identifier`) para
    procesar cada columna origen una sola vez en vez de una vez por campo.
    Para campos con `secondary_identifier` distinto de la columna origen
    (records donde varias definiciones de campo comparten una misma
    columna `tcsn` pero corresponden a sub-tipos distintos, distinguidos
    por un valor fijo en una posición del record), filtra primero las
    filas cuyo contenido en `secondary_identifier_pos`/`_len` coincide con
    el `secondary_identifier` esperado, y solo extrae el campo de esas
    filas — el resto queda vacío para esa columna en ese chunk. Cada campo
    se extrae con slicing de string por `position`/`length` (ambos
    1-indexados en la definición).

    Args:
        data: DataFrame del chunk crudo (una columna por `tcsn`/columna de
            transform, valores string de ancho fijo).
        field_defs: DataFrame de definiciones de campo ya cargado y
            ordenado (ver `_load_field_definitions`).
        type_record: Tipo de record (no usado directamente en la lógica,
            solo pasado por compatibilidad de firma).

    Returns:
        DataFrame con una columna por campo extraído (más `record` como
        primera columna si `data` la trae), o un DataFrame vacío si
        `data`/`field_defs` están vacíos o no se pudo extraer ningún
        campo.

    Ejemplo:
        _extract_fields(chunk_df, field_defs, 'draft')  # DataFrame con ~90 columnas
    """
    if data.empty or field_defs.empty:
        return pd.DataFrame()

    from collections import defaultdict
    tcsn_groups = defaultdict(list)
    for fd in field_defs.itertuples():
        tcsn = str(fd.tcsn) if hasattr(fd, 'tcsn') else ''
        if tcsn and tcsn in data.columns:
            tcsn_groups[tcsn].append(fd)
        else:
            sec_id = str(fd.secondary_identifier).strip() \
                     if hasattr(fd, 'secondary_identifier') \
                     and fd.secondary_identifier \
                     and not pd.isna(fd.secondary_identifier) else ''
            if sec_id and sec_id in data.columns:
                tcsn_groups[sec_id].append(fd)

    fields = []

    for tcsn, fds in tcsn_groups.items():
        col = data[tcsn]

        for fd in fds:
            position    = int(fd.position)    if hasattr(fd, 'position')    else 0
            length      = int(fd.length)      if hasattr(fd, 'length')      else 0
            column_name = str(fd.column_name) if hasattr(fd, 'column_name') else ''

            if not column_name or position <= 0 or length <= 0:
                continue

            sec_id = fd.secondary_identifier \
                     if hasattr(fd, 'secondary_identifier') else None

            sec_id_str = str(sec_id).strip() if sec_id and not pd.isna(sec_id) else ''

            # ← CAMBIO: agrega "or tcsn == sec_id_str" para detectar caso SMS
            # SMS: tcsn fue reasignado a "22200", sec_id_str también es "22200"
            # → son iguales → no hay filtro adicional de filas
            if not sec_id_str or tcsn == sec_id_str:
                col_view = col
            else:
                sec_id_pos = int(fd.secondary_identifier_pos) \
                             if hasattr(fd, 'secondary_identifier_pos') \
                             and fd.secondary_identifier_pos else 0
                sec_id_len = int(fd.secondary_identifier_len) \
                             if hasattr(fd, 'secondary_identifier_len') \
                             and fd.secondary_identifier_len else 0

                if sec_id_pos > 0 and sec_id_len > 0:
                    try:
                        mask     = col.str.slice(sec_id_pos-1, sec_id_pos-1+sec_id_len) == sec_id_str
                        col_view = col[mask]
                    except Exception:
                        col_view = col
                else:
                    col_view = col

            try:
                field = pd.Series(
                    col_view.str.slice(
                        start=position - 1,
                        stop=position - 1 + length
                    ).reindex(data.index, fill_value=''),
                    name=column_name
                )
                fields.append(field)
            except Exception:
                continue

    if not fields:
        return pd.DataFrame()

    extract_df = pd.concat(fields, axis=1).fillna('').astype(str)
    extract_df = extract_df.reset_index(drop=True)

    if 'record' in data.columns:
        extract_df.insert(0, 'record', data['record'].reset_index(drop=True).values)

    return extract_df


# =============================================================================
# FUNCIÓN PRINCIPAL DE EXTRACCIÓN POR OUTPUT
# =============================================================================

def extract_output(
    output: Dict,
    client_id: str, brand: str,
    file_type: str, file_date: str, content_hash: str
) -> Optional[Dict]:
    """
    Extrae un output de transform (un `type_record` de un archivo, ej.
    BASEII o VSS_120) de punta a punta: carga las definiciones de campo,
    descarga el Parquet de transform, lo procesa en streaming por chunks
    de `EXTRACT_CHUNK_SIZE` filas (`iter_batches` + `ParquetWriter`, para
    no cargar el archivo completo en memoria), inserta `content_hash` como
    primera columna de cada chunk extraído y sube el Parquet consolidado a
    `s3-staging/200_*_ext_*/`.

    Args:
        output: Dict de un output de transform, con `output_type`,
            `s3_key` y `records`.
        client_id: Código del cliente (no usado para lógica, solo
            trazabilidad).
        brand: Marca del archivo ("VISA"), no usado para lógica, solo
            trazabilidad.
        file_type: "IN" u "OUT", no usado para lógica, solo trazabilidad.
        file_date: Fecha del archivo (no usado para lógica, solo
            trazabilidad).
        content_hash: MD5 del archivo origen, insertado como primera
            columna de cada chunk extraído.

    Returns:
        Dict con el resultado de la extracción (`output_type`, `s3_key`,
        `records`, `fields`, `batches`), o `None` si el `output_type` no
        está en `OUTPUT_TYPE_CONFIG`, no hay definiciones de campo para su
        `type_record`, o no se extrajo ningún registro. Relanza cualquier
        excepción tras loguearla.

    Ejemplo:
        extract_output({'output_type': 'BASEII', 's3_key': '...', 'records': 1000},
                        'EBGR', 'VISA', 'IN', '2026-01-03', 'AB12...')
    """
    output_type   = output.get('output_type')
    input_s3_key  = output.get('s3_key')
    input_records = output.get('records', 0)

    logger.info(f"{'='*60}")
    logger.info(f"Processing extract for: {output_type}")

    config = OUTPUT_TYPE_CONFIG.get(output_type)
    if not config:
        return None

    type_record        = config['type_record']
    input_subdir       = config['input_subdir']
    output_subdir      = config['output_subdir']
    sort_by            = config['sort_by']
    special_processing = config.get('special_processing')

    try:
        t0 = time.time()
        field_defs = _load_field_definitions(type_record, sort_by)
        logger.info(f"  [TIMING] DynamoDB load: {time.time()-t0:.2f}s ({len(field_defs)} fields)")

        if field_defs.empty:
            return None

        if special_processing == 'sms':
            field_defs = _process_sms_field_defs(field_defs)

        t1 = time.time()
        file_obj = _get_s3_file_object(input_s3_key)
        logger.info(f"  [TIMING] S3 download: {time.time()-t1:.2f}s")

        parquet_file  = pq.ParquetFile(file_obj)
        output_s3_key = input_s3_key.replace(input_subdir, output_subdir)
        output_buffer = BytesIO()
        writer        = None
        records_written = 0
        fields_count    = 0
        batch_num       = 0

        logger.info(f"Starting chunked processing. Chunk size: {CHUNK_SIZE:,}")

        for batch in parquet_file.iter_batches(batch_size=CHUNK_SIZE):
            chunk_df = batch.to_pandas()
            if chunk_df.empty:
                continue

            batch_num += 1
            t_batch = time.time()

            t_conv = time.time()
            extracted_chunk = _extract_fields(chunk_df, field_defs, type_record)
            t_extract = time.time() - t_conv

            if extracted_chunk.empty:
                continue

            extracted_chunk.insert(0, 'content_hash', content_hash)

            t_arrow = time.time()
            extracted_table = pa.Table.from_pandas(extracted_chunk)
            t_arrow = time.time() - t_arrow

            if writer is None:
                writer       = pq.ParquetWriter(output_buffer, extracted_table.schema, compression='snappy')
                fields_count = len(extracted_chunk.columns)

            t_write = time.time()
            writer.write_table(extracted_table)
            t_write = time.time() - t_write

            records_written += len(extracted_chunk)
            t_total = time.time() - t_batch
            logger.info(f"  Batch {batch_num}: +{len(extracted_chunk):,} records "
                        f"(total: {records_written:,}) | "
                        f"extract={t_extract:.2f}s arrow={t_arrow:.2f}s write={t_write:.2f}s total={t_total:.2f}s")

        if writer is None:
            logger.warning(f"No valid records for {output_type}")
            return None

        writer.close()
        output_buffer.seek(0)

        t_upload = time.time()
        logger.info(f"Uploading to S3: {output_s3_key}")
        s3.put_object(Bucket=STAGING_BUCKET, Key=output_s3_key, Body=output_buffer.getvalue())
        logger.info(f"  [TIMING] S3 upload: {time.time()-t_upload:.2f}s")
        logger.info(f"Done: {records_written:,} records, {fields_count} fields, {batch_num} batches")

        file_obj.close()
        output_buffer.close()

        return {
            'output_type':   output_type,
            'type_record':   type_record,
            'input_subdir':  input_subdir,
            'output_subdir': output_subdir,
            'input_s3_key':  input_s3_key,
            's3_key':        output_s3_key,
            'input_records': input_records,
            'records':       records_written,
            'fields':        fields_count,
            'batches':       batch_num,
        }

    except Exception as e:
        logger.error(f"Error extracting {output_type}: {str(e)}", exc_info=True)
        raise


# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Punto de entrada de la Lambda `lmbd-vi-extract`. Invocada por la Step
    Function Visa tras `lmbd-vi-transform`. Recorre todos los outputs de
    transform recibidos en el evento, extrae cada uno con
    `extract_output()` de forma independiente (un fallo en un output no
    aborta los demás) y agrega el resultado en un único payload de salida
    para la siguiente etapa (`lmbd-vi-clean`).

    Args:
        event: Payload de Step Functions con `client_id`, `file_id`,
            `brand`, `file_type`, `file_date`, `content_hash` y `outputs`
            (lista de outputs de transform a extraer).
        context: Contexto de ejecución de Lambda (no usado).

    Returns:
        Dict con `status` ("SUCCESS", "PARTIAL_SUCCESS" o "ERROR"),
        `total_outputs`, `total_records`, `total_fields`, la lista
        `outputs` con el resultado de cada extracción exitosa, y `errors`
        con el detalle de los outputs que fallaron. Lanza `ValueError` si
        falta `S3_BUCKET_STAGING`.

    Ejemplo:
        lambda_handler({'client_id': 'EBGR', 'file_id': '...', 'brand': 'VISA',
                         'file_type': 'IN', 'file_date': '2026-01-03',
                         'content_hash': '...', 'outputs': [...]}, None)
    """
    logger.info("=" * 70)
    logger.info("ITX EXTRACT LAMBDA v2 - START")
    logger.info(f"Config: chunk_size={CHUNK_SIZE:,}")
    logger.info("=" * 70)

    if not STAGING_BUCKET:
        raise ValueError("Missing: S3_BUCKET_STAGING")

    client_id    = event.get('client_id')
    file_id      = event.get('file_id')
    brand        = event.get('brand')
    file_type    = event.get('file_type')
    file_date    = event.get('file_date')
    content_hash = event.get('content_hash')

    transform_outputs = event.get('outputs', [])
    if not transform_outputs:
        return {'status': 'SUCCESS', 'outputs': []}

    extract_outputs = []
    errors          = []

    for output in transform_outputs:
        try:
            result = extract_output(
                output=output,
                client_id=client_id, brand=brand,
                file_type=file_type, file_date=file_date,
                content_hash=content_hash
            )
            if result:
                extract_outputs.append(result)
        except Exception as e:
            errors.append({
                'output_type': output.get('output_type'),
                'error': str(e)
            })

    total_records = sum(o.get('records', 0) for o in extract_outputs)
    total_fields  = sum(o.get('fields',  0) for o in extract_outputs)
    total_batches = sum(o.get('batches', 0) for o in extract_outputs)

    status = ('ERROR'           if (errors and not extract_outputs) else
              'PARTIAL_SUCCESS' if errors else
              'SUCCESS')

    logger.info(f"=== Done: {len(extract_outputs)} outputs, "
                f"{total_records:,} records, "
                f"{total_batches} batches total ===")

    return {
        'status':        status,
        'total_outputs': len(extract_outputs),
        'total_records': total_records,
        'total_fields':  total_fields,
        'outputs':       extract_outputs,
        'errors':        errors if errors else None,
        'client_id':     client_id,
        'file_id':       file_id,
        'brand':         brand,
        'file_type':     file_type,
        'file_date':     file_date,
        'content_hash':  content_hash
    }