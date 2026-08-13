"""
handler.py — Lambda real: itl-0004-itx-dev-intchg-02-lmbd-unzip
================================================================================
Archivo:     lambdas/unzip/src/handler.py

Descomprime archivos ZIP detectados por el router antes de que lleguen al
flujo normal del pipeline. Sube al landing **todos** los archivos internos,
matcheen o no un patrón de `file_pattern` en DynamoDB — la clasificación
sigue consultándose acá (mismos patrones que usa el router) pero solo para
el caso especial de nombrado de VISA ARDEF (ver `_extract_and_upload()`);
para todo lo demás es solo informativa (log `unmatched`). Archiva el ZIP
original en s3-archive (no en s3-operational) y elimina el ZIP del landing.
Cada archivo extraído y subido dispara al router nuevamente via S3 Event —
es la forma en que el pipeline logra paralelismo gratis al procesar los N
archivos internos de un ZIP sin orquestación adicional, y también la forma
en que un archivo sin match de patrón termina pasando por el manejo de
"desconocido" del router (`procesar_archivo_desconocido()`, ver
gotchas.md) en vez de perderse silenciosamente acá adentro (2026-08-12 —
antes de este fix, un archivo sin match se descartaba en esta misma etapa,
sin llegar nunca a s3-landing y por lo tanto sin que el router lo viera).

Flujo:
  1. Recibe el S3 key del ZIP y la file_date extraída por el router
  2. Descarga el ZIP en streaming a /tmp (chunks de 8MB)
  3. Inspecciona los archivos internos sin extraer (solo lee el índice)
  4. Consulta patrones de DynamoDB — mismos que usa el router — solo para
     resolver el nombrado especial de VISA ARDEF, no para filtrar
  5. Extrae y sube TODOS los archivos al landing, matcheen o no
  6. Archiva el ZIP original → archive/originals/zip/{year}/{month}/
  7. Elimina el ZIP del landing
  8. Los archivos subidos al landing disparan el router via S3 Event
     automáticamente → paralelismo gratis sin configuración adicional;
     el router decide la clasificación real (o UNKNOWN) de cada uno

Formatos de ZIP recibidos:
  MAST260416.zip          → Mastercard, YYMMDD en nombre
  VISA260416.zip          → Visa, YYMMDD en nombre
  20260416visaout.zip     → Visa OUT, YYYYMMDD en nombre
  20260416visain.zip      → Visa IN, YYYYMMDD en nombre
  20260416mcin.zip        → Mastercard IN, YYYYMMDD en nombre

Variables de entorno:
  S3_BUCKET_LANDING           : bucket de destino para archivos extraídos
  S3_BUCKET_ARCHIVE           : bucket para archivar el ZIP original
  DYNAMODB_TABLE_FILE_PATTERN : tabla de patrones (default: itx-file-pattern)
  EXTRACT_CHUNK_SIZE_MB       : chunk para descargar el ZIP (default: 8MB)
"""

import os
import re
import zipfile
import logging
import boto3
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3       = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

LANDING_BUCKET      = os.environ.get('S3_BUCKET_LANDING')
ARCHIVE_BUCKET      = os.environ.get('S3_BUCKET_ARCHIVE')
FILE_PATTERN_TABLE  = os.environ.get('DYNAMODB_TABLE_FILE_PATTERN', 'itx-file-pattern')
EXTRACT_CHUNK_BYTES = int(os.environ.get('EXTRACT_CHUNK_SIZE_MB', '8')) * 1024 * 1024

MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100MB


# =============================================================================
# PATRONES DESDE DYNAMODB
# Misma lógica que el router — garantiza clasificación consistente
# =============================================================================

def _load_patterns(customer_code: str) -> List[Dict]:
    """
    Carga patrones activos de DynamoDB para el cliente (incluyendo los
    patrones genéricos `customer_code='ALL'`), ordenados por prioridad.
    Misma lógica que el router para garantizar consistencia — un archivo
    dentro de un ZIP debe clasificarse igual que si hubiera llegado suelto
    al landing.

    Args:
        customer_code: Código del cliente, ej. "EBGR".

    Returns:
        Lista de patrones (dicts) ordenados por `priority` ascendente, ya
        filtrados por cliente. Lista vacía si no hay patrones activos o si
        falla la consulta a DynamoDB (el error se loguea, no se relanza).

    Ejemplo:
        _load_patterns("EBGR")  # -> [{"file_format": "...", "brand": "VISA", ...}, ...]
    """
    try:
        table    = dynamodb.Table(FILE_PATTERN_TABLE)
        response = table.scan(
            FilterExpression='is_active = :active',
            ExpressionAttributeValues={':active': 1}
        )
        items = response.get('Items', [])

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='is_active = :active',
                ExpressionAttributeValues={':active': 1},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        if not items:
            logger.warning("No active patterns in DynamoDB")
            return []

        items.sort(key=lambda x: int(x.get('priority', 999)))
        items = [p for p in items if p.get('customer_code') in [customer_code, 'ALL']]

        logger.info(f"Loaded {len(items)} patterns for '{customer_code}'")
        return items

    except Exception as e:
        logger.error(f"Error loading patterns: {str(e)}")
        return []


def _matches_pattern(filename: str, patterns: List[Dict]) -> Optional[Dict]:
    """
    Retorna la clasificación del primer patrón (ya ordenados por prioridad)
    cuyo regex hace match con el nombre de archivo, o None si ninguno
    matchea.

    Args:
        filename: Nombre del archivo interno del ZIP a clasificar.
        patterns: Lista de patrones ya cargados y ordenados por prioridad
            (ver `_load_patterns`).

    Returns:
        Dict con `brand`, `direction`, `pattern_id` y `customer_code` del
        primer patrón que matchea, o None si no hay match o el regex de
        algún patrón es inválido (se ignora ese patrón y se sigue probando).

    Ejemplo:
        _matches_pattern("I479273260330", patterns)
        # -> {"brand": "VISA", "direction": "IN", "pattern_id": "1", "customer_code": "EBGR"}
    """
    for patron in patterns:
        regex = patron.get('file_format', '')
        if not regex:
            continue
        try:
            if re.search(regex, filename, re.IGNORECASE):
                return {
                    'brand':         patron.get('brand', 'UNKNOWN'),
                    'direction':     patron.get('direction', 'UNKNOWN'),
                    'pattern_id':    patron.get('pattern_id'),
                    'customer_code': patron.get('customer_code'),
                }
        except re.error:
            continue
    return None


# =============================================================================
# DESCARGA DEL ZIP A /TMP EN STREAMING
# =============================================================================

def _download_zip_to_tmp(bucket: str, key: str, tmp_path: str) -> int:
    """
    Descarga el ZIP de S3 a /tmp en chunks de EXTRACT_CHUNK_BYTES.
    Nunca carga el ZIP completo en RAM.

    Args:
        bucket: Bucket origen (landing).
        key: Key del ZIP en el bucket origen.
        tmp_path: Ruta local en /tmp donde se escribe el ZIP descargado.

    Returns:
        Tamaño descargado en bytes.

    Ejemplo:
        _download_zip_to_tmp("itx-landing-dev", "EBGR/VISA260416.zip",
            "/tmp/VISA260416.zip")  # -> 52428800
    """
    logger.info(f"Downloading ZIP: s3://{bucket}/{key} → {tmp_path}")

    response  = s3.get_object(Bucket=bucket, Key=key)
    file_size = response.get('ContentLength', 0)
    bytes_dl  = 0

    with open(tmp_path, 'wb') as f:
        body = response['Body']
        while True:
            chunk = body.read(EXTRACT_CHUNK_BYTES)
            if not chunk:
                break
            f.write(chunk)
            bytes_dl += len(chunk)

    logger.info(f"Downloaded {bytes_dl / 1024 / 1024:.1f}MB "
                f"({file_size / 1024 / 1024:.1f}MB expected)")
    return bytes_dl


# =============================================================================
# INSPECCIÓN DEL ZIP SIN EXTRAER
# =============================================================================

def _inspect_zip(tmp_path: str) -> List[str]:
    """
    Lista los archivos dentro del ZIP sin extraerlos — solo lee el índice
    central del ZIP. Filtra carpetas (entradas que terminan en "/") y
    archivos ocultos (nombre que empieza con ".").

    Args:
        tmp_path: Ruta local del ZIP ya descargado.

    Returns:
        Lista de nombres de entrada (paths dentro del ZIP) procesables —
        excluye carpetas y ocultos.

    Ejemplo:
        _inspect_zip("/tmp/VISA260416.zip")
        # -> ["I479273260330", "I479273260331", ...]
    """
    with zipfile.ZipFile(tmp_path, 'r') as zf:
        all_names = zf.namelist()

    files = [
        name for name in all_names
        if not name.endswith('/')
        and not name.split('/')[-1].startswith('.')
        and name.split('/')[-1]
    ]

    logger.info(f"ZIP contains {len(all_names)} entries → {len(files)} processable files")
    for f in files:
        logger.info(f"  {f}")

    return files


# =============================================================================
# EXTRACCIÓN Y SUBIDA AL LANDING
# =============================================================================

def _extract_and_upload(
    tmp_zip_path: str,
    zip_entry_name: str,
    client_id: str,
    dest_bucket: str,
    pattern_id: str = None,
    file_date: str  = None
) -> str:
    """
    Extrae un archivo del ZIP y lo sube al landing, sin escribirlo a disco
    intermedio — lee la entrada del ZIP en streaming y la sube directo a S3.
    Para archivos pequeños: upload simple. Para archivos grandes (>100MB):
    multipart upload, acumulando en buffer hasta alcanzar 10MB por parte
    (mínimo de S3 es 5MB).

    Destino en landing: {client_id}/{filename} — salvo `pattern_id == '7'`
    (patrón VISA ARDEF, cuyo nombre de archivo no trae fecha), donde se
    antepone `file_date` al nombre para evitar colisiones entre versiones.

    Args:
        tmp_zip_path: Ruta local del ZIP ya descargado.
        zip_entry_name: Path de la entrada dentro del ZIP a extraer.
        client_id: Código del cliente, usado como prefijo del key destino.
        dest_bucket: Bucket destino (landing).
        pattern_id: ID del patrón que clasificó este archivo — condiciona el
            key destino si es "7" (VISA ARDEF).
        file_date: Fecha de negocio, antepuesta al nombre solo si
            `pattern_id == '7'`.

    Returns:
        S3 key del archivo subido al landing.

    Ejemplo:
        _extract_and_upload("/tmp/VISA260416.zip", "I479273260330", "EBGR",
            "itx-landing-dev")  # -> "EBGR/I479273260330"
    """
    filename = zip_entry_name.split('/')[-1]
    if pattern_id == '7': # Solo para patrones con fecha en el nombre (VISA ARDEF)
        dest_key = f"{client_id}/{file_date}_{filename}"
    else:
        dest_key = f"{client_id}/{filename}"

    with zipfile.ZipFile(tmp_zip_path, 'r') as zf:
        file_size = zf.getinfo(zip_entry_name).file_size

        logger.info(f"  Uploading: {filename} ({file_size / 1024 / 1024:.1f}MB) "
                    f"→ s3://{dest_bucket}/{dest_key}")

        if file_size < MULTIPART_THRESHOLD:
            # Upload simple para archivos < 100MB
            with zf.open(zip_entry_name) as entry:
                s3.put_object(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    Body=entry.read()
                )
        else:
            # Multipart upload para archivos >= 100MB
            mpu       = s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
            upload_id = mpu['UploadId']
            parts     = []
            part_num  = 1
            buffer    = b''

            try:
                with zf.open(zip_entry_name) as entry:
                    while True:
                        chunk = entry.read(EXTRACT_CHUNK_BYTES)
                        if not chunk:
                            break
                        buffer += chunk

                        # Subir parte cuando alcanza 10MB (mínimo S3 = 5MB)
                        if len(buffer) >= 10 * 1024 * 1024:
                            response = s3.upload_part(
                                Bucket=dest_bucket,
                                Key=dest_key,
                                UploadId=upload_id,
                                PartNumber=part_num,
                                Body=buffer
                            )
                            parts.append({'PartNumber': part_num, 'ETag': response['ETag']})
                            logger.info(f"    Part {part_num}: {len(buffer) / 1024 / 1024:.1f}MB")
                            part_num += 1
                            buffer    = b''

                    # Subir el resto final
                    if buffer:
                        response = s3.upload_part(
                            Bucket=dest_bucket,
                            Key=dest_key,
                            UploadId=upload_id,
                            PartNumber=part_num,
                            Body=buffer
                        )
                        parts.append({'PartNumber': part_num, 'ETag': response['ETag']})

                s3.complete_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )

            except Exception as e:
                s3.abort_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id
                )
                raise

    logger.info(f"  Uploaded: {dest_key}")
    return dest_key


# =============================================================================
# ARCHIVO DEL ZIP ORIGINAL
# =============================================================================

def _archive_zip(
    source_bucket: str,
    source_key: str,
    client_id: str,
    file_date: str,
    dest_bucket: str
) -> str:
    """
    Archiva el ZIP original en el bucket de archive, bajo
    originals/zip/{year}/{month}/. Usa `copy_object` server-side — no
    descarga ni vuelve a subir el archivo, la copia ocurre íntegramente
    dentro de S3. Si `file_date` no se puede parsear, usa la fecha actual
    como fallback.

    Estructura:
      {client_id}/originals/zip/{year}/{month}/{zip_filename}

    Args:
        source_bucket: Bucket origen del ZIP (landing).
        source_key: Key del ZIP en el bucket origen.
        client_id: Código del cliente, usado como prefijo del key destino.
        file_date: Fecha de negocio en formato "YYYY-MM-DD".
        dest_bucket: Bucket destino (archive).

    Returns:
        S3 key donde quedó archivado el ZIP.

    Ejemplo:
        _archive_zip("itx-landing-dev", "EBGR/VISA260416.zip", "EBGR",
            "2026-04-16", "itl-...-s3-archive")
        # -> "EBGR/originals/zip/2026/04/VISA260416.zip"
    """
    zip_filename = source_key.split('/')[-1]

    try:
        dt    = datetime.strptime(file_date, "%Y-%m-%d")
        year  = dt.strftime("%Y")
        month = dt.strftime("%m")
    except (ValueError, TypeError):
        now   = datetime.utcnow()
        year  = now.strftime("%Y")
        month = now.strftime("%m")

    archive_key = f"{client_id}/originals/zip/{year}/{month}/{zip_filename}"

    logger.info(f"Archiving ZIP → s3://{dest_bucket}/{archive_key}")
    s3.copy_object(
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Bucket=dest_bucket,
        Key=archive_key
    )
    logger.info("ZIP archived")
    return archive_key


def _delete_from_landing(bucket: str, key: str) -> None:
    """
    Elimina el ZIP original de landing. Se llama solo después de haberlo
    archivado en s3-archive (ver `_archive_zip`).

    Args:
        bucket: Bucket de landing.
        key: Key del ZIP a eliminar.

    Returns:
        None.

    Ejemplo:
        _delete_from_landing("itx-landing-dev", "EBGR/VISA260416.zip")
    """
    logger.info(f"Deleting ZIP from landing: s3://{bucket}/{key}")
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info("Landing clean")


def _cleanup_tmp(tmp_path: str) -> None:
    """
    Elimina el ZIP temporal de /tmp para liberar espacio. No lanza excepción
    si falla — solo registra un warning.

    Args:
        tmp_path: Ruta local a eliminar.

    Returns:
        None.

    Ejemplo:
        _cleanup_tmp("/tmp/VISA260416.zip")
    """
    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    except Exception as e:
        logger.warning(f"Could not clean tmp: {str(e)}")


# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Entry point del Lambda unzip. Invocado asincrónicamente por el router
    cuando detecta un ZIP en el landing. Descarga el ZIP, inspecciona su
    índice sin extraer nada a disco, consulta los patrones de DynamoDB
    (solo para el nombrado especial de VISA ARDEF, ver
    `_extract_and_upload()`) y sube **todos** los archivos internos al
    landing, matcheen o no un patrón — los que no matchean quedan
    contados en `unmatched`, pero se suben igual, para que el router los
    vea y los registre como `UNKNOWN` en vez de perderse acá (ver
    `procesar_archivo_desconocido()` en `lambdas/router/src/handler.py`).
    Archiva el ZIP original en s3-archive y limpia el landing. Cada
    archivo subido dispara al router nuevamente vía S3 Event, logrando
    paralelismo sin orquestación adicional. El bloque `finally` limpia
    /tmp tanto en éxito como en fallo.

    Args:
        event: Payload recibido desde el router:
            {
                "client_id":      "EBGR",
                "bucket_landing": "itx-landing-dev",
                "s3_key":         "EBGR/VISA260416.zip",
                "file_date":      "2026-04-16"
            }
        context: Contexto de ejecución de Lambda (no usado por el handler).

    Returns:
        Dict con el resultado de la descompresión:
        {
            "status":        "EXTRACTED",
            "zip_file":      "VISA260416.zip",
            "total_in_zip":  10,
            "uploaded":      10,
            "unmatched":     6,
            "uploaded_keys": ["EBGR/I479273260330", ...],
            "archive_key":   "EBGR/originals/zip/2026/04/VISA260416.zip"
        }
        Lanza `ValueError` si faltan `S3_BUCKET_LANDING`/`S3_BUCKET_ARCHIVE`,
        campos requeridos del evento, o no hay patrones activos para el
        cliente.

    Ejemplo:
        lambda_handler({"client_id": "EBGR", "s3_key": "EBGR/VISA260416.zip",
            "file_date": "2026-04-16"}, None)
        # -> {"status": "EXTRACTED", "zip_file": "VISA260416.zip", ...}
    """
    logger.info("=" * 60)
    logger.info("ITX UNZIP LAMBDA - START")
    logger.info(f"Config: chunk={EXTRACT_CHUNK_BYTES // 1024 // 1024}MB")
    logger.info("=" * 60)

    if not LANDING_BUCKET:
        raise ValueError("Missing: S3_BUCKET_LANDING")
    if not ARCHIVE_BUCKET:
        raise ValueError("Missing: S3_BUCKET_ARCHIVE")

    client_id      = event.get('client_id')
    bucket_landing = event.get('bucket_landing', LANDING_BUCKET)
    s3_key         = event.get('s3_key')
    file_date      = event.get('file_date', datetime.utcnow().strftime("%Y-%m-%d"))

    if not all([client_id, s3_key]):
        raise ValueError(f"Missing required: client_id={client_id}, s3_key={s3_key}")

    zip_filename = s3_key.split('/')[-1]
    tmp_path     = f"/tmp/{zip_filename}"

    logger.info(f"Processing ZIP: {zip_filename}")
    logger.info(f"  Client:    {client_id}")
    logger.info(f"  Source:    s3://{bucket_landing}/{s3_key}")
    logger.info(f"  File date: {file_date}")

    try:
        # Paso 1 — Cargar patrones desde DynamoDB. Un cliente sin NINGÚN
        # patrón activo (a diferencia de "tiene patrones pero este archivo
        # no matchea ninguno") ya NO aborta el ZIP completo (2026-08-12) —
        # antes, un `raise` acá cortaba todo antes de descargar/extraer/
        # archivar, dejando el ZIP entero atascado en landing para
        # siempre. `_matches_pattern(filename, [])` ya devuelve None de
        # forma natural con una lista vacía, así que cada archivo interno
        # simplemente cae en la rama "sin match" (ver Paso 4) y sube igual
        # — el router los va a registrar como UNKNOWN.
        patterns = _load_patterns(client_id)
        if not patterns:
            logger.warning(
                f"Sin patrones activos para '{client_id}' — todos los "
                f"archivos del ZIP se van a subir sin clasificar (UNKNOWN)"
            )

        # Paso 2 — Descargar ZIP a /tmp en streaming
        _download_zip_to_tmp(bucket_landing, s3_key, tmp_path)

        # Paso 3 — Inspeccionar contenido sin extraer
        all_files = _inspect_zip(tmp_path)

        # Paso 4 — Extraer y subir TODOS los archivos internos, matcheen o no
        # un patrón. Antes, un archivo sin match se descartaba acá mismo
        # (nunca llegaba a s3-landing) — el router nunca lo veía, así que su
        # manejo de "sin clasificar" (procesar_archivo_desconocido(), ver
        # gotchas.md) no aplicaba a nada que llegara comprimido. La
        # clasificación sigue haciéndose acá SOLO para el caso especial de
        # `_extract_and_upload()` (pattern_id == "7", VISA ARDEF, antepone
        # file_date al nombre) — para todo lo demás, matchee o no, sube con
        # el nombre tal cual y deja que el router sea la única fuente de
        # verdad de la clasificación (evita 2 lugares con la misma lógica
        # de patrones que pueden desalinearse, como pasó acá).
        uploaded_keys = []
        unmatched     = []

        for zip_entry in all_files:
            filename      = zip_entry.split('/')[-1]
            clasificacion = _matches_pattern(filename, patterns)

            if clasificacion:
                logger.info(f"  MATCH: {filename} "
                            f"({clasificacion['brand']}/{clasificacion['direction']})")
                pattern_id = clasificacion['pattern_id']
            else:
                logger.info(f"  SIN MATCH: {filename} — se sube igual, "
                            f"el router lo va a registrar como UNKNOWN")
                pattern_id = None
                unmatched.append(filename)

            dest_key = _extract_and_upload(
                tmp_zip_path=tmp_path,
                zip_entry_name=zip_entry,
                client_id=client_id,
                dest_bucket=bucket_landing,
                pattern_id=pattern_id,
                file_date=file_date
            )
            uploaded_keys.append(dest_key)

        logger.info(f"Summary: {len(uploaded_keys)} uploaded ({len(unmatched)} sin match de patrón)")

        # Paso 5 — Archivar ZIP original en archive (server-side copy)
        archive_key = _archive_zip(
            source_bucket=bucket_landing,
            source_key=s3_key,
            client_id=client_id,
            file_date=file_date,
            dest_bucket=ARCHIVE_BUCKET
        )

        # Paso 6 — Eliminar ZIP del landing
        _delete_from_landing(bucket_landing, s3_key)

        logger.info(f"=== Unzip complete — {len(uploaded_keys)} files sent to landing ===")
        logger.info(f"    S3 Events trigger router automatically for each file")

        return {
            'status':        'EXTRACTED',
            'zip_file':      zip_filename,
            'file_date':     file_date,
            'total_in_zip':  len(all_files),
            'uploaded':      len(uploaded_keys),
            'unmatched':     len(unmatched),
            'uploaded_keys': uploaded_keys,
            'archive_key':   archive_key,
        }

    finally:
        _cleanup_tmp(tmp_path)