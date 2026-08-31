"""
handler.py — Lambda real: itl-0004-itx-{env}-intchg-02-lmbd-vi-ardef
================================================================================
Archivo:     lambdas/visa/ardef/src/handler.py

Punto de entrada del motor de reglas ARDEF de Visa (rangos de BINes y reglas
de fees). El router invoca este Lambda de forma asíncrona (InvocationType=
'Event', sin Step Functions — ver decision.md "Por qué ARDEF e IAR no usan
Step Functions") cuando clasifica un archivo con direction=ARDEF. Orquesta
las 5 etapas internas del pipeline ARDEF (interpreter → transform → clean →
calculate → operational), que van dejando el archivo en distintos subdirs de
STAGING hasta consolidar la tabla maestra acumulada lu_ardef en REFERENCE —
la que usan las Lambdas de calculate de Visa (calc_ardef) para cruzar
transacciones contra el rango de BIN/regla vigente en la fecha de la
transacción.

Flujo:
1. Recibir evento del router (payload en variables_input, ver
   `_extract_event_params`)
2. Validar file_id / file_processing_date
3. Ejecutar interpretate_ardef() → transform_ardef() → clean_ardef() →
   calculate_ardef() → load_operational_ardef(), en ese orden
4. Liberar memoria (gc.collect) al finalizar

Variables de entorno:
  ITX_CLIENT_ID                : client_id por defecto (default: DEMO)
  ITX_ENVIRONMENT              : nombre del ambiente (default: development)
  ITX_S3_BUCKET_LANDING        : bucket de landing
  ITX_S3_BUCKET_STAGING        : bucket de staging (etapas intermedias RAW/TRA/CLN/CAL)
  ITX_S3_BUCKET_OPERATIONAL    : bucket de operational (salida final)
  ITX_S3_BUCKET_REFERENCE      : bucket de reference (tabla maestra lu_ardef)
  ITX_TABLE_FILE_CONTROL       : tabla DynamoDB de control de archivos
  ITX_LOG_LEVEL                : nivel de logging (default: info)
"""

import gc
import json
import logging

from ardef import calculate, clean, interpreter, operational, transform
from ardef.persistence.file import FileStorage

# Logger estándar
# Lambda captura automáticamente todo lo que va a stout/stderr
# y lo envía a CloudWatch logs sin configuración adicional.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

layer = FileStorage.Layer


def _pipeline_ardef(file_id: str, file_processing_date: str) -> None:
    """
    Orquesta las 5 etapas del pipeline ARDEF en orden: interpreter → transform
    → clean → calculate → operational. Cada etapa lee el output de la
    anterior desde STAGING y escribe la suya en el mismo bucket, hasta que
    `calculate_ardef` actualiza la tabla maestra lu_ardef en REFERENCE y
    `load_operational_ardef` consolida el resultado final en OPERATIONAL.

    Args:
        file_id: ID único del archivo asignado por el router (MD5 del
            contenido o variante si es una nueva versión).
        file_processing_date: Fecha de negocio del archivo, formato
            "YYYY-MM-DD".

    Returns:
        None. Lanza cualquier excepción de las etapas internas sin capturarla
        — el try/except de `lambda_handler` es quien la maneja.

    Ejemplo:
        _pipeline_ardef("1606e40fdc88e10521c619ef69666528", "2026-04-24")
    """
    interpreter.interpretate_ardef(
        origin_layer=layer.LANDING,
        target_layer=layer.STAGING,
        file_id=file_id,
        file_processing_date=file_processing_date
    )
    
    transform.transform_ardef(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    clean.clean_ardef(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    calculate.calculate_ardef(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    operational.load_operational_ardef(
        origin_layer=layer.STAGING,
        target_layer=layer.OPERATIONAL,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )

    gc.collect()


def _extract_event_params(event:dict) -> tuple[str, str]:
    """
    Extrae y valida file_id y file_processing_date del evento recibido del
    router.

    El router invoca este Lambda con InvocationType='Event' (asíncrono)
    pasando el siguiente payload en variables_input:

        {
            "client_id":      "BTRLRO",
            "file_id":        "1606e40fdc88e10521c619ef69666528",
            "filename":       "20260424repository.ardef.txt",
            "s3_key_landing": "BTRLRO/20260424repository.ardef.txt",
            "bucket_landing": "itl-0004-itx-dev-poc-02-landing",
            "brand":          "VISA",
            "brand_id":       "VI",
            "file_type":      "ARDEF",
            "file_date":      "2026-04-24",       <-- clave que usa el router
            "content_hash":   "ABC123...",
        }

    Nota: el router usa la clave 'file_date', pero internamente el pipeline
    ARDEF la maneja como 'file_processing_date' — el mapeo ocurre acá.

    Args:
        event: Payload del router tal cual llega al handler (dict de arriba).

    Returns:
        Tupla (file_id, file_processing_date), ambos strings ya validados
        como no vacíos.

    Ejemplo:
        _extract_event_params({"file_id": "ABC123", "file_date": "2026-04-24"})
        # -> ("ABC123", "2026-04-24")
    """
    file_id: str = event.get("file_id", "").strip()

    # El router usa 'file_date'; en ardef usa usa 'file_processing_date'
    file_processing_date: str = event.get("file_date", "").strip()

    missing = []
    if not file_id:
        missing.append("file_id")
    if not file_processing_date:
        missing.append("file_date")

    if missing:
        raise ValueError(
            f"Payload inválido - faltan campos obligatorios: {', '.join(missing)}. "
            f"Event recibido: {json.dumps(event)}"
        )
    
    return file_id, file_processing_date

def lambda_handler(event, context):
    """
    Punto de entrada del Lambda.

    Invocado asincrónamente por el router (VISA_ARDEF_FUNCTION_NAME)
    cuando detecta un archivo con direction=ARDEF

    En este punto el router ha:
        - Registrado el archivo en DynamoDB (file_control) con status=PROCESSING
        - Extraído la fecha del dehader ARDEF vía extrar_fecha_ardef()
        - Calculado el content_hash y verificado que no es duplicado

    Args:
        event: dict con el payload del router (ver `_extract_event_params`).
        context: objeto con info de la ejecución Lambda (tiempo restante, etc.),
            no se usa directamente en este handler.

    Returns:
        dict con statusCode 200 si el pipeline completa sin errores,
        400 si el payload está mal formado,
        500 si ocurre un error durante el pipeline. El body siempre incluye
        file_id, file_processing_date, client_id y filename para trazabilidad
        en CloudWatch.

    Ejemplo:
        lambda_handler({"file_id": "ABC123", "file_date": "2026-04-24", ...}, context)
        # -> {"statusCode": 200, "body": '{"message": "Pipeline ARDEF completado..."}'}
    """
    logger.info("=== Inicio pipeline ARDEF ===")
    logger.info(f"Event recibido: {json.dumps(event)}")

    # Extrar y validar parámetros del evento
    try:
        file_id, file_processing_date = _extract_event_params(event)
    except ValueError as exc:
        logger.error(f"Payload inválido: {exc}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(exc)})
        }
    
    # log de contexto
    logger.info(
        f"Iniciando pipeline | "
        f"file={file_id} | "
        f"file_processing_date={file_processing_date} | "
        f"client_id={event.get('client_id', 'N/A')} | "
        f"filename={event.get('filename', 'N/A')} | "
        f"brand={event.get('brand', 'N/A')} | "
        f"s3_key_landing={event.get('s3_key_landing', 'N/A')}"
    )

    try:
        _pipeline_ardef(
            file_id=file_id,
            file_processing_date=file_processing_date,
        )

        logger.info("=== Pipeline ARDEF completado exitosamente ===")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Pipeline ARDEF completado exitosamente",
                "file_id": file_id,
                "file_processing_date": file_processing_date,
                "client_id": event.get("client_id"),
                "filename": event.get("filename"),
            }),
        }
    
    except Exception as exc:
        logger.error(f"Error en pipeline en ARDEF: {exc}", exc_info=True)

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": f"Error: {str(exc)}",
                "file_id": file_id,
                "file_processing_date": file_processing_date,
                "client_id": event.get("client_id"),
                "filename": event.get("filename"),
            }),
        }