"""
handler.py — Lambda real: itl-0004-itx-dev-intchg-02-lmbd-archive-file
================================================================================
Archivo:     lambdas/archive-file/src/handler.py

Último paso del pipeline de interchange (Visa y Mastercard) — se ejecuta
SIEMPRE al final, tanto en éxito como en fallo. Comprime el archivo original
recibido en el landing y lo mueve, ya comprimido en ZIP, al bucket de
archive (s3-archive), dejando el landing limpio para el siguiente archivo.

Estrategia de compresión (streaming):
  - Lee el archivo de S3 en chunks de COMPRESS_CHUNK_SIZE (nunca > 8MB en RAM)
  - Escribe directamente al ZIP en /tmp usando zipfile.ZIP_DEFLATED
  - Sube el ZIP a operational usando multipart upload (para archivos > 100MB)
  - Elimina el original de landing solo después de verificar el ZIP en destino

Por qué ZIP y no GZ:
  - Ya tienes itx-unzip — formato consistente en todo el sistema
  - Python nativo: no requiere librerías externas
  - Compatible con herramientas estándar del negocio bancario

Por qué /tmp y no BytesIO:
  - Un archivo de 1.5GB comprime a ~200MB
  - /tmp de Lambda tiene 512MB por defecto (suficiente para el ZIP)
  - BytesIO cargaría el ZIP completo en RAM — innecesario

Estructura de destino:
  {client_id}/originals/{brand}/{file_type}/{year}/{month}/{filename}.zip

Ejemplo:
  EBGR/originals/VISA/IN/2026/04/I479273260330.zip
  SBSA/originals/VISA/OUT/2026/04/VISA_Outward_Settlement_SBSA_20260404.TXT.zip

Variables de entorno:
  S3_BUCKET_LANDING        : bucket origen (landing)
  S3_BUCKET_ARCHIVE         : bucket destino (Archive)
  COMPRESS_CHUNK_SIZE_MB   : tamaño de chunk para comprimir (default: 8MB)
"""

import os
import zipfile
import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

LANDING_BUCKET       = os.environ.get('S3_BUCKET_LANDING')
ARCHIVE_BUCKET   = os.environ.get('S3_BUCKET_ARCHIVE')
COMPRESS_CHUNK_BYTES = int(os.environ.get('COMPRESS_CHUNK_SIZE_MB', '32')) * 1024 * 1024

# Umbral para usar multipart upload (100MB)
MULTIPART_THRESHOLD = 100 * 1024 * 1024


# =============================================================================
# CONSTRUCCIÓN DEL PATH DE DESTINO
# =============================================================================

def _build_archive_key(
    client_id: str,
    brand: str,
    file_type: str,
    file_date: str,
    filename: str
) -> str:
    """
    Construye el S3 key de destino en el bucket de archive, agrupando por
    cliente/marca/tipo de archivo/año/mes. Si `file_date` no se puede parsear
    (formato inesperado o vacío), usa la fecha actual como fallback en vez de
    fallar el archivado completo.

    Estructura:
      {client_id}/originals/{brand}/{file_type}/{year}/{month}/{filename}.zip

    Args:
        client_id: Código del cliente, ej. "EBGR".
        brand: Marca del archivo, "VISA" o "MASTERCARD".
        file_type: Dirección del archivo, "IN" u "OUT".
        file_date: Fecha de negocio en formato "YYYY-MM-DD".
        filename: Nombre del archivo original (sin comprimir).

    Returns:
        S3 key de destino, con extensión ".zip" agregada si el archivo
        original no la tenía ya.

    Ejemplo:
      _build_archive_key("EBGR", "VISA", "IN", "2026-04-03", "I479273260330")
      # -> "EBGR/originals/VISA/IN/2026/04/I479273260330.zip"
    """
    try:
        dt    = datetime.strptime(file_date, "%Y-%m-%d")
        year  = dt.strftime("%Y")
        month = dt.strftime("%m")
    except (ValueError, TypeError):
        now   = datetime.utcnow()
        year  = now.strftime("%Y")
        month = now.strftime("%m")
        logger.warning(f"Could not parse file_date '{file_date}' — using current date")

    # Agregar .zip si el archivo original no es ya un ZIP
    zip_filename = filename if filename.lower().endswith('.zip') else f"{filename}.zip"

    return f"{client_id}/originals/{brand}/{file_type}/{year}/{month}/{zip_filename}"


# =============================================================================
# COMPRESIÓN EN STREAMING
# =============================================================================

def _compress_and_save_to_tmp(
    source_bucket: str,
    source_key: str,
    filename: str,
    tmp_path: str
) -> int:
    """
    Lee el archivo de S3 en chunks y lo escribe comprimido en /tmp.
    Nunca tiene más de COMPRESS_CHUNK_BYTES en RAM al mismo tiempo — necesario
    porque los archivos de interchange pueden superar 1.5 GB.

    Args:
        source_bucket: Bucket origen (landing).
        source_key: Key del archivo original dentro del bucket origen.
        filename: Nombre con el que se registra la entrada dentro del ZIP.
        tmp_path: Ruta local en /tmp donde se escribe el ZIP resultante.

    Returns:
        Tamaño del ZIP resultante en bytes.

    Ejemplo:
        _compress_and_save_to_tmp("itx-landing-dev", "EBGR/I479273260330",
            "I479273260330", "/tmp/I479273260330.zip")  # -> 209715200
    """
    logger.info(f"Compressing s3://{source_bucket}/{source_key}")
    logger.info(f"  Chunk size: {COMPRESS_CHUNK_BYTES // 1024 // 1024}MB")
    logger.info(f"  Destination: {tmp_path}")

    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    body     = response['Body']
    file_size = response.get('ContentLength', 0)

    logger.info(f"  Source size: {file_size / 1024 / 1024:.1f}MB")

    bytes_read = 0

    with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        with zf.open(filename, 'w') as zip_entry:
            while True:
                chunk = body.read(COMPRESS_CHUNK_BYTES)
                if not chunk:
                    break
                zip_entry.write(chunk)
                bytes_read += len(chunk)

    zip_size = os.path.getsize(tmp_path)
    compression_ratio = (1 - zip_size / file_size) * 100 if file_size > 0 else 0

    logger.info(f"  Bytes read:      {bytes_read / 1024 / 1024:.1f}MB")
    logger.info(f"  ZIP size:        {zip_size / 1024 / 1024:.1f}MB")
    logger.info(f"  Compression:     {compression_ratio:.1f}% reduction")

    return zip_size


# =============================================================================
# SUBIDA A S3 — SIMPLE O MULTIPART SEGÚN TAMAÑO
# =============================================================================

def _upload_zip_to_s3(tmp_path: str, dest_bucket: str, dest_key: str, zip_size: int) -> None:
    """
    Sube el ZIP desde /tmp a S3.

    Para archivos < 100MB: put_object simple
    Para archivos >= 100MB: multipart upload
      → recomendado por AWS para archivos grandes
      → más eficiente en red y más tolerante a fallos

    Args:
        tmp_path: Ruta local del ZIP a subir.
        dest_bucket: Bucket destino (archive).
        dest_key: Key destino dentro del bucket de archive.
        zip_size: Tamaño del ZIP en bytes, usado para decidir la estrategia
            de subida.

    Returns:
        None. Si el multipart upload falla, aborta la subida en curso
        (`abort_multipart_upload`) para no dejar partes huérfanas en S3 y
        relanza la excepción original.

    Ejemplo:
        _upload_zip_to_s3("/tmp/file.zip", "itl-...-s3-archive",
            "EBGR/originals/VISA/IN/2026/04/file.zip", 157286400)
    """
    if zip_size < MULTIPART_THRESHOLD:
        logger.info(f"Uploading (simple): s3://{dest_bucket}/{dest_key}")
        with open(tmp_path, 'rb') as f:
            s3.put_object(
                Bucket=dest_bucket,
                Key=dest_key,
                Body=f,
                ContentType='application/zip'
            )
    else:
        logger.info(f"Uploading (multipart): s3://{dest_bucket}/{dest_key}")
        mpu = s3.create_multipart_upload(
            Bucket=dest_bucket,
            Key=dest_key,
            ContentType='application/zip'
        )
        upload_id = mpu['UploadId']
        parts     = []
        part_num  = 1

        try:
            with open(tmp_path, 'rb') as f:
                while True:
                    chunk = f.read(MULTIPART_THRESHOLD)  # partes de 100MB
                    if not chunk:
                        break
                    response = s3.upload_part(
                        Bucket=dest_bucket,
                        Key=dest_key,
                        UploadId=upload_id,
                        PartNumber=part_num,
                        Body=chunk
                    )
                    parts.append({
                        'PartNumber': part_num,
                        'ETag': response['ETag']
                    })
                    logger.info(f"  Part {part_num} uploaded ({len(chunk) / 1024 / 1024:.1f}MB)")
                    part_num += 1

            s3.complete_multipart_upload(
                Bucket=dest_bucket,
                Key=dest_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            logger.info(f"Multipart upload completed: {part_num - 1} parts")

        except Exception as e:
            # Cancelar el multipart upload para no dejar partes huérfanas en S3
            logger.error(f"Multipart upload failed — aborting: {str(e)}")
            s3.abort_multipart_upload(
                Bucket=dest_bucket,
                Key=dest_key,
                UploadId=upload_id
            )
            raise


# =============================================================================
# VERIFICACIÓN Y LIMPIEZA
# =============================================================================

def _verify_upload(bucket: str, key: str) -> bool:
    """
    Verifica que el ZIP existe en S3 antes de eliminar el original de
    landing — evita borrar el archivo fuente si la subida falló o quedó
    incompleta.

    Args:
        bucket: Bucket donde debería estar el ZIP subido (archive).
        key: Key del ZIP a verificar.

    Returns:
        True si el objeto existe en S3, False si `head_object` falla por
        cualquier motivo (no encontrado, permisos, etc.).

    Ejemplo:
        _verify_upload("itl-...-s3-archive", "EBGR/originals/.../file.zip")
        # -> True
    """
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        logger.error(f"Verification failed for s3://{bucket}/{key}: {str(e)}")
        return False


def _delete_from_landing(bucket: str, key: str) -> None:
    """
    Elimina el archivo original de landing. Solo se llama tras verificación
    exitosa del ZIP en destino (ver `_verify_upload`).

    Args:
        bucket: Bucket de landing.
        key: Key del archivo original a eliminar.

    Returns:
        None.

    Ejemplo:
        _delete_from_landing("itx-landing-dev", "EBGR/I479273260330")
    """
    logger.info(f"Deleting from landing: s3://{bucket}/{key}")
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info("Deleted from landing — landing is now clean")


def _cleanup_tmp(tmp_path: str) -> None:
    """
    Elimina el archivo temporal de /tmp para liberar espacio del entorno de
    ejecución. No lanza excepción si falla — solo registra un warning.

    Args:
        tmp_path: Ruta local a eliminar.

    Returns:
        None.

    Ejemplo:
        _cleanup_tmp("/tmp/I479273260330.zip")
    """
    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            logger.info(f"Cleaned up tmp: {tmp_path}")
    except Exception as e:
        logger.warning(f"Could not clean tmp file: {str(e)}")


# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Entry point del Lambda archive. Comprime el archivo original del landing
    y lo sube al bucket de archive, verifica que la subida se completó
    correctamente, y solo entonces elimina el original de landing. Si la
    verificación falla, deja el archivo original intacto en landing y lanza
    `RuntimeError` en vez de borrarlo. El bloque `finally` limpia /tmp tanto
    en éxito como en fallo.

    Args:
        event: Payload recibido desde Step Functions (paso PrepareArchiveInput):
            {
                "file_id":        "ABC123",
                "client_id":      "EBGR",
                "brand":          "VISA",
                "file_type":      "IN",
                "file_date":      "2026-04-03",
                "s3_key_landing": "EBGR/I479273260330",
                "bucket_landing": "itx-landing-dev"
            }
        context: Contexto de ejecución de Lambda (no usado por el handler).

    Returns:
        Dict con el resultado del archivado:
        {
            "status":      "ARCHIVED",
            "file_id":     "ABC123",
            "archive_key": "EBGR/originals/VISA/IN/2026/04/I479273260330.zip"
        }
        Lanza `ValueError` si falta `S3_BUCKET_ARCHIVE` o algún campo
        requerido del evento; `RuntimeError` si la verificación post-subida
        falla.

    Ejemplo:
        lambda_handler({"file_id": "ABC123", "client_id": "EBGR",
            "brand": "VISA", "file_type": "IN", "file_date": "2026-04-03",
            "s3_key_landing": "EBGR/I479273260330"}, None)
        # -> {"status": "ARCHIVED", "file_id": "ABC123", "archive_key": "..."}
    """
    logger.info("=" * 60)
    logger.info("ITX ARCHIVE LAMBDA - START")
    logger.info(f"Config: chunk={COMPRESS_CHUNK_BYTES // 1024 // 1024}MB")
    logger.info("=" * 60)

    if not ARCHIVE_BUCKET:
        raise ValueError("Missing environment variable: S3_BUCKET_ARCHIVE")

    file_id        = event.get('file_id')
    client_id      = event.get('client_id')
    brand          = event.get('brand')
    file_type      = event.get('file_type')
    file_date      = event.get('file_date')
    s3_key_landing = event.get('s3_key_landing')
    bucket_landing = event.get('bucket_landing', LANDING_BUCKET)

    if not all([file_id, client_id, brand, file_type, file_date, s3_key_landing]):
        raise ValueError(
            f"Missing required fields — received: "
            f"file_id={file_id}, client_id={client_id}, brand={brand}, "
            f"file_type={file_type}, file_date={file_date}, "
            f"s3_key_landing={s3_key_landing}"
        )

    filename    = s3_key_landing.split('/')[-1]
    archive_key = _build_archive_key(client_id, brand, file_type, file_date, filename)
    tmp_path    = f"/tmp/{filename}.zip"

    logger.info(f"Archiving: {filename}")
    logger.info(f"  Source:  s3://{bucket_landing}/{s3_key_landing}")
    logger.info(f"  Dest:    s3://{ARCHIVE_BUCKET}/{archive_key}")

    try:
        # Paso 1 — Comprimir en streaming a /tmp
        zip_size = _compress_and_save_to_tmp(
            source_bucket=bucket_landing,
            source_key=s3_key_landing,
            filename=filename,
            tmp_path=tmp_path
        )

        # Paso 2 — Subir ZIP a operational (simple o multipart según tamaño)
        _upload_zip_to_s3(tmp_path, ARCHIVE_BUCKET, archive_key, zip_size)

        # Paso 3 — Verificar que el ZIP existe en destino
        if not _verify_upload(ARCHIVE_BUCKET, archive_key):
            raise RuntimeError(
                f"Archive verification failed — ZIP not found at destination. "
                f"Landing file preserved: s3://{bucket_landing}/{s3_key_landing}"
            )

        # Paso 4 — Eliminar de landing (solo si verificación fue exitosa)
        _delete_from_landing(bucket_landing, s3_key_landing)

        logger.info(f"=== Archived successfully: {archive_key} ===")

        return {
            'status':      'ARCHIVED',
            'file_id':     file_id,
            'archive_key': archive_key,
        }

    finally:
        # Limpiar /tmp siempre — éxito o fallo
        _cleanup_tmp(tmp_path)