"""
handler.py — Lambda real: itl-0004-itx-{env}-intchg-02-lmbd-router
================================================================================
Archivo:     lambdas/router/src/handler.py

Punto de entrada del pipeline de interchange. Se dispara por un evento S3
ObjectCreated cuando llega un archivo nuevo al bucket de landing. Extrae el
client_id del path, clasifica el archivo contra los patrones regex
configurados en DynamoDB (file_pattern), calcula su MD5 en streaming para
detectar duplicados o nuevas versiones del mismo archivo, extrae la fecha de
negocio del contenido (con lógica distinta según sea Visa, Mastercard, IAR o
ARDEF) y registra el archivo en DynamoDB (file_control). Según la
clasificación, despacha el archivo a la siguiente etapa: la Step Function de
Visa o de Mastercard para el flujo transaccional normal (IN/OUT), directamente
a las Lambdas de reglas (vi-ardef / mc-iar) sin pasar por Step Functions, o a
la Lambda de descompresión (unzip) si el archivo es un ZIP.

Flujo:
1. Parsear evento S3 → bucket/key
2. Extraer client_id del path
3. Detectar si es ZIP → delegar a itx-unzip asincrónicamente
4. Cargar patrones de DynamoDB
5. Clasificar archivo con regex
6. Extraer fecha del header (solo 50 bytes para Visa; lectura completa
   para Mastercard bloqueado — ver extraer_fecha_mc)
7. Calcular MD5 en streaming (sin cargar todo el archivo en memoria)
8. Verificar duplicado en DynamoDB
9. Registrar en DynamoDB
10. Iniciar Step Functions (o invocar Lambda directa para ARDEF/IAR)

Variables de entorno:
  S3_BUCKET_LANDING             : bucket de landing
  DYNAMODB_TABLE_FILE_CONTROL   : tabla de control (default: itx-file-control)
  DYNAMODB_TABLE_FILE_PATTERN   : tabla de patrones (default: itx-file-pattern)
  STEP_FUNCTION_VI_ARN          : ARN de la Step Function Visa
  STEP_FUNCTION_MC_ARN          : ARN de la Step Function Mastercard
  VISA_ARDEF_FUNCTION_NAME      : ARN/nombre de la Lambda Visa ARDEF
  MASTERCARD_IAR_FUNCTION_NAME  : ARN/nombre de la Lambda Mastercard IAR
  UNZIP_FUNCTION_NAME           : nombre de la Lambda unzip (default: itx-unzip)
"""

import os
import re
import json
import hashlib
import logging
import boto3
import io
import struct
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote_plus


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3            = boto3.client('s3')
dynamodb      = boto3.resource('dynamodb')
sfn           = boto3.client('stepfunctions')
lambda_client = boto3.client('lambda')

LANDING_BUCKET      = os.environ.get('S3_BUCKET_LANDING')
ARCHIVE_BUCKET      = os.environ.get('S3_BUCKET_ARCHIVE')
TABLE_FILE_CONTROL  = os.environ.get('DYNAMODB_TABLE_FILE_CONTROL', 'itx-file-control')
TABLE_FILE_PATTERN  = os.environ.get('DYNAMODB_TABLE_FILE_PATTERN', 'itx-file-pattern')
UNZIP_FUNCTION_NAME = os.environ.get('UNZIP_FUNCTION_NAME', 'itx-unzip')
STEP_FUNCTION_VI_ARN   = os.environ.get('STEP_FUNCTION_VI_ARN')
STEP_FUNCTION_MC_ARN   = os.environ.get('STEP_FUNCTION_MC_ARN')
VISA_ARDEF_FUNCTION_NAME   = os.environ.get('VISA_ARDEF_FUNCTION_NAME')
MASTERCARD_IAR_FUNCTION_NAME   = os.environ.get('MASTERCARD_IAR_FUNCTION_NAME')

HASH_CHUNK_SIZE = 1 * 1024 * 1024
# Mismos valores que lmbd-archive-file — ver _archivar_bajo_prefijo(), que
# porta su patrón de compresión en streaming para archivos UNKNOWN/DUPLICATE.
COMPRESS_CHUNK_BYTES = int(os.environ.get('COMPRESS_CHUNK_SIZE_MB', '32')) * 1024 * 1024
MULTIPART_THRESHOLD = 100 * 1024 * 1024
FILE_TYPE_MAP = {
    'IN': 'IN',
    'INCOMING': 'IN',
    'OUT': 'OUT',
    'OUTGOING': 'OUT',
    'IAR': 'IAR',
    'ARDEF': 'ARDEF',
}
BRAND_ID_MAP = {
    'VISA': 'VI',
    'MASTERCARD': 'MC',
}

# =============================================================================
# VALIDACIÓN DE VARIABLES DE ENTORNO
# =============================================================================
def validar_configuracion():
    
    """
    Verifica que todas las variables de entorno requeridas por el router
    estén configuradas antes de procesar cualquier evento — Step Function
    ARNs, Lambda ARNs (ARDEF/IAR/unzip) y nombres de tabla DynamoDB.

    Returns:
        None. Lanza ValueError si falta alguna variable requerida.

    Ejemplo:
        validar_configuracion()  # raise ValueError si falta STEP_FUNCTION_VI_ARN
    """
    required_env_vars = {
        'STEP_FUNCTION_VI_ARN': STEP_FUNCTION_VI_ARN,
        'STEP_FUNCTION_MC_ARN' : STEP_FUNCTION_MC_ARN,
        'VISA_ARDEF_FUNCTION_NAME': VISA_ARDEF_FUNCTION_NAME,
        'MASTERCARD_IAR_FUNCTION_NAME': MASTERCARD_IAR_FUNCTION_NAME,
        'UNZIP_FUNCTION_NAME': UNZIP_FUNCTION_NAME,
        'TABLE_FILE_CONTROL': TABLE_FILE_CONTROL,
        'TABLE_FILE_PATTERN': TABLE_FILE_PATTERN,
    }

    missing_vars = [
        name for name, value in required_env_vars.items()
        if not value
    ]

    if missing_vars:
        raise ValueError(
            "Faltan variables de entorno requeridas: "
            + ", ".join(missing_vars)
        )

# =============================================================================
# DETECCIÓN Y DELEGACIÓN DE ZIPs
# =============================================================================

def _is_zip_file(filename: str) -> bool:
    """
    Detecta si el archivo es un ZIP por su extensión.

    Args:
        filename: Nombre del archivo a evaluar.

    Returns:
        True si el nombre termina en ".zip" (case-insensitive).

    Ejemplo:
        _is_zip_file("VISA260416.ZIP")  # True
    """
    return filename.lower().endswith('.zip')


def _extraer_fecha_de_zip(filename: str) -> str:
    """
    Extrae la fecha de negocio a partir del nombre del archivo ZIP, antes de
    descomprimirlo, para poder invocar la Lambda de unzip con esa fecha.

    Soporta dos formatos presentes en los clientes:
      YYYYMMDD: 20260416visaout.zip → 2026-04-16
                20260416mcin.zip    → 2026-04-16
      YYMMDD:   MAST260416.zip      → 2026-04-16
                VISA260416.zip      → 2026-04-16

    Estrategia:
      1. Buscar YYYYMMDD primero (8 dígitos) — más específico
      2. Si no → buscar YYMMDD (6 dígitos)
      3. Si no → fecha actual como fallback

    Args:
        filename: Nombre del archivo ZIP, ej. "VISA260416.zip".

    Returns:
        Fecha en formato "YYYY-MM-DD", o la fecha actual si no se pudo extraer.

    Ejemplo:
        _extraer_fecha_de_zip("20260416visaout.zip")  # "2026-04-16"
    """
    fecha_default = datetime.utcnow().strftime("%Y-%m-%d")

    # Intentar YYYYMMDD (8 dígitos consecutivos)
    match = re.search(r'(\d{8})', filename)
    if match:
        try:
            dt    = datetime.strptime(match.group(1), '%Y%m%d')
            fecha = dt.strftime('%Y-%m-%d')
            logger.info(f"  Fecha ZIP (YYYYMMDD): {match.group(1)} → {fecha}")
            return fecha
        except ValueError:
            pass  # no era fecha válida, seguir buscando

    # Intentar YYMMDD (6 dígitos consecutivos)
    match = re.search(r'(\d{6})', filename)
    if match:
        try:
            dt    = datetime.strptime(match.group(1), '%y%m%d')
            fecha = dt.strftime('%Y-%m-%d')
            logger.info(f"  Fecha ZIP (YYMMDD): {match.group(1)} → {fecha}")
            return fecha
        except ValueError:
            pass

    logger.warning(f"  No se pudo extraer fecha de '{filename}' → usando fecha actual")
    return fecha_default


def _handle_zip(
    bucket: str,
    key: str,
    client_id: str,
    file_date: str
) -> Dict:
    """
    Delega el procesamiento del ZIP a itx-unzip de forma asíncrona.

    Por qué asíncrono (InvocationType='Event'):
      - El router no espera que el unzip termine
      - El unzip puede tardar varios minutos (ZIPs de 1-2GB)
      - Los archivos extraídos dispararán el router nuevamente
        via S3 Event automáticamente → paralelismo gratis

    Args:
        bucket: Bucket S3 donde está el ZIP (landing).
        key: Key del ZIP dentro del bucket.
        client_id: Código de cliente, extraído del path.
        file_date: Fecha de negocio ya extraída del nombre del ZIP
            (_extraer_fecha_de_zip), propagada al payload de unzip.

    Returns:
        Dict con 'file', 'status'='DELEGATED_TO_UNZIP', 'file_date' — no es
        el resultado final del procesamiento, solo confirma que se delegó.

    Ejemplo:
        _handle_zip(bucket, "EBGR/VISA260416.zip", "EBGR", "2026-04-16")
    """
    payload = {
        'client_id':      client_id,
        'bucket_landing': bucket,
        's3_key':         key,
        'file_date':      file_date,
    }

    logger.info(f"  ZIP detectado → delegando a {UNZIP_FUNCTION_NAME} (async)")
    logger.info(f"  file_date extraída: {file_date}")

    lambda_client.invoke(
        FunctionName=UNZIP_FUNCTION_NAME,
        InvocationType='Event',
        Payload=json.dumps(payload).encode()
    )

    logger.info(f"  itx-unzip invocado — router continúa sin esperar")

    return {
        'file':      key.split('/')[-1],
        'status':    'DELEGATED_TO_UNZIP',
        'file_date': file_date,
    }


# =============================================================================
# IDENTIFICACIÓN DE ARCHIVOS
# =============================================================================

def generar_file_id(client_id: str, filename: str) -> str:
    """
    Genera un ID determinista basado en el nombre del archivo — el mismo
    archivo (mismo client_id + filename + fecha en el nombre) siempre produce
    el mismo file_id, lo que permite detectar duplicados por nombre en
    verificar_duplicado().

    Args:
        client_id: Código de cliente.
        filename: Nombre del archivo (se busca una fecha de 8 dígitos dentro
            del nombre; si no hay, usa el literal "NODATE").

    Returns:
        MD5 en hexadecimal, mayúsculas, de "{client_id}|{filename}|{fecha}".

    Ejemplo:
        generar_file_id("EBGR", "VS.EBGR.TC00.20260103.001.txt")
    """
    match = re.search(r"(\d{8})", filename)
    fecha = match.group(1) if match else "NODATE"
    texto = f"{client_id}|{filename}|{fecha}"
    return hashlib.md5(texto.encode()).hexdigest().upper()


def generar_file_id_unico(client_id: str, filename: str, content_hash: str) -> str:
    """
    Genera un file_id nuevo cuando llega el mismo nombre de archivo con
    contenido diferente (mismo generar_file_id pero content_hash distinto al
    ya registrado) — incorpora el content_hash para garantizar unicidad entre
    versiones del mismo archivo.

    Args:
        client_id: Código de cliente.
        filename: Nombre del archivo.
        content_hash: MD5 del contenido del archivo (se usan los primeros 16
            caracteres).

    Returns:
        MD5 en hexadecimal, mayúsculas, de "{client_id}|{filename}|{content_hash[:16]}".

    Ejemplo:
        generar_file_id_unico("EBGR", "VS.EBGR.TC00.20260103.001.txt", "AB12CD34...")
    """
    texto = f"{client_id}|{filename}|{content_hash[:16]}"
    return hashlib.md5(texto.encode()).hexdigest().upper()


def calcular_content_hash(bucket: str, key: str) -> str:
    """
    Calcula el MD5 del archivo en streaming, sin cargarlo completo en RAM.

    Por qué streaming:
      El método anterior hacía response['Body'].read() que descarga el archivo
      completo en memoria. Para archivos de 1.5GB esto puede causar OOM o
      timeout en el Lambda Router, resultando en content_hash = "" y
      generando nombres de archivo ".parquet" que Spark ignora silenciosamente.

    Estrategia:
      1. Intentar usar el S3 ETag si el archivo fue subido en un PUT simple
         (el ETag es el MD5 cuando no hay multipart upload).
      2. Si el ETag tiene el sufijo "-N" (multipart), calcular MD5 en streaming
         leyendo chunks de 1MB. Nunca hay más de 1MB en RAM.

    Args:
        bucket: Bucket S3 donde está el archivo.
        key: Key del archivo.

    Returns:
        MD5 en hexadecimal, mayúsculas. Cadena vacía si falla el cálculo (el
        caller debe usar file_id como fallback — ver el paso correspondiente
        de lambda_handler).

    Ejemplo:
        calcular_content_hash(bucket, "EBGR/VS.EBGR.TC00.20260103.001.txt")
    """
    try:
        # Obtener metadata sin descargar el archivo
        head = s3.head_object(Bucket=bucket, Key=key)
        etag = head.get('ETag', '').strip('"')

        # ETag sin sufijo "-N" → es el MD5 real del contenido completo
        if etag and '-' not in etag:
            logger.info(f"  content_hash: usando S3 ETag (no multipart)")
            return etag.upper()

        # ETag con "-N" → multipart upload, calcular MD5 en streaming
        logger.info(f"  content_hash: streaming MD5 (multipart)")
        md5      = hashlib.md5()
        response = s3.get_object(Bucket=bucket, Key=key)
        body     = response['Body']

        while True:
            chunk = body.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            md5.update(chunk)

        return md5.hexdigest().upper()

    except Exception as e:
        logger.error(f"Error calculando content_hash de s3://{bucket}/{key}: {e}")
        # IMPORTANTE: No retornar "" — usar file_id como fallback garantiza
        # que el nombre del Parquet nunca sea ".parquet" (archivo oculto).
        # El caller debe pasar file_id como fallback.
        return ""


def obtener_file_size(bucket: str, key: str, event_size: int = 0) -> int:
    """
    Obtiene el tamaño del archivo. Usa el evento S3 como fallback
    para evitar un request extra si el evento ya trae el dato.

    Args:
        bucket: Bucket S3 donde está el archivo.
        key: Key del archivo.
        event_size: Tamaño ya reportado por el evento S3 (0 si no viene o es
            desconocido).

    Returns:
        Tamaño en bytes (event_size si es > 0, sino un head_object; 0 si
        falla).

    Ejemplo:
        obtener_file_size(bucket, key, event_size=104857600)  # 104857600
    """
    if event_size > 0:
        return event_size
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except Exception as e:
        logger.warning(f"Error obteniendo size: {e}")
        return 0


# =============================================================================
# DETECCIÓN DE FECHA DEL ARCHIVO
# =============================================================================

def convertir_fecha_juliana(texto_juliano: str) -> Optional[str]:
    """
    Convierte formato YYDDD a YYYY-MM-DD.
    YY = año (00-99), DDD = día del año (001-365).

    Args:
        texto_juliano: String de 5 dígitos (YYDDD) a convertir.

    Returns:
        Fecha en formato "YYYY-MM-DD", o None si texto_juliano no tiene
        exactamente 5 dígitos o no es una fecha juliana válida.

    Ejemplo:
        convertir_fecha_juliana("26004")  # "2026-01-04"
    """
    if not texto_juliano or not texto_juliano.isdigit() or len(texto_juliano) != 5:
        return None
    try:
        dt = datetime.strptime(texto_juliano, "%y%j")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def extraer_fecha(bucket: str, key: str) -> str:
    """
    Extrae la fecha de procesamiento del header del archivo CTF (Visa).

    Lee solo los primeros 50 bytes (Range request) para no descargar
    el archivo completo. Funciona para archivos CTF 168 y VMS 170 chars.

    Posición de la fecha juliana (YYDDD):
      CTF 168: posición 8:13 de la línea
      VMS 170: idem, pero la línea tiene 2 bytes extra al inicio (pos 2-4)
               → ajustamos leyendo desde la posición 10:15

    Args:
        bucket: Bucket S3 donde está el archivo.
        key: Key del archivo Visa CTF.

    Returns:
        Fecha en formato "YYYY-MM-DD", o la fecha actual (UTC) como fallback
        si el header es demasiado corto o no se pudo detectar la fecha
        juliana en ninguna de las 2 posiciones probadas.

    Ejemplo:
        extraer_fecha(bucket, "EBGR/VS.EBGR.TC00.20260103.001.txt")
    """
    fecha_default = datetime.utcnow().strftime("%Y-%m-%d")

    try:
        response = s3.get_object(Bucket=bucket, Key=key, Range='bytes=0-49')
        cabecera  = response['Body'].read().decode('latin-1')

        if len(cabecera) < 13:
            logger.warning("Header demasiado corto")
            return fecha_default
        
        # Detectar formato VMS (170 chars → primer carácter desplazado)
        # Intentar en posición 8:13 (CTF) y 10:15 (VMS) como fallback
        for start, end in [(8, 13), (10, 15)]:
            texto_juliano = cabecera[start:end]
            fecha = convertir_fecha_juliana(texto_juliano)
            if fecha:
                logger.info(f"  Fecha detectada pos[{start}:{end}] ({texto_juliano}): {fecha}")
                return fecha

        logger.warning(f"No se pudo detectar fecha juliana. Raw header: {cabecera[:20]!r}")
        return fecha_default

    except Exception as e:
        logger.error(f"Error leyendo fecha: {e}")
        return fecha_default

def extraer_fecha_iar(bucket: str, key: str) -> str:
    """
    Extrae la fecha de procesamiento del header de un archivo IAR
    (Mastercard) en S3, leyendo solo los primeros 100 bytes. El primer
    registro viene precedido por 4 bytes de longitud (big-endian); según la
    longitud decodificada del registro (27 u 80 caracteres) la fecha está en
    una posición y formato distintos.

    Args:
        bucket: Bucket S3 donde está el archivo.
        key: Key del archivo IAR.

    Returns:
        Fecha en formato "YYYY-MM-DD".

    Raises:
        ValueError: si no se puede leer la longitud del primer registro, la
            longitud es inválida, el header está incompleto, o su longitud
            decodificada no es 27 ni 80 caracteres (formato desconocido).

    Ejemplo:
        extraer_fecha_iar(bucket, "EBGR/IAR.EBGR.20260103.txt")
    """
    encoding: str = "latin1"
    
    response = s3.get_object(
        Bucket=bucket,
        Key=key,
        Range="bytes=0-99",
    )
    
    file_bytes = response["Body"].read()
    stream = io.BytesIO(file_bytes)
    raw_len = stream.read(4)
    if len(raw_len) < 4:
        raise ValueError("No se pudo leer la longitud del primer registro.")

    record_length = struct.unpack(">i", raw_len)[0]
    if record_length <= 0:
        raise ValueError(f"Longitud inválida del primer registro: {record_length}")

    raw_record = stream.read(record_length)
    if len(raw_record) < record_length:
        raise ValueError(
            f"Header incompleto. Esperado={record_length}, leído={len(raw_record)}"
        )
        
    record_raw = raw_record.decode(encoding)
    if len(record_raw) == 27:
        raw_date = record_raw[15:23].strip()
        input_format = "%Y%m%d"
    elif len(record_raw) == 80:
        raw_date = record_raw[45:54].replace("/", "").strip()
        input_format = "%m%d%y"
    else:
        raise ValueError(
            f"Header desconocido. Longitud detectada: {len(record_raw)}"
        )

    return datetime.strptime(raw_date, input_format).strftime("%Y-%m-%d")

def extraer_fecha_ardef(bucket: str, key:str) -> str: # POR TESTEAR
    """
    Extrae la fecha del header del archivo ARDEF leyendo solo los primeros 32Kb.

    Busca líneas con el patrón de cabecera ARDEF:
    posición 0-8: 'AAACTRNG'
    posición 10-17: 'AEPACRN'
    posición 23-31: fecha en formato YYYYMMDD (ardef_header_date)
    posición 63-67: número de versión

    Si hay varias líneas de cabecera (distintas versiones), retorna
    la fecha de la versión más alta - mismo criterio que vi_interpreter.

    Args:
        bucket: Bucket S3 donde está el archivo.
        key: Key del archivo ARDEF.

    Returns:
        Fecha en formato "YYYY-MM-DD", o la fecha actual (UTC) como fallback
        si no se encuentra ningún header ARDEF válido en los primeros 32KB o
        la fecha detectada no es parseable.

    Ejemplo:
        extraer_fecha_ardef(bucket, "EBGR/ARDEF.EBGR.20260103.txt")
    """
    fecha_default = datetime.utcnow().strftime("%Y-%m-%d")
    CHUNK_BYTES = 32 * 1024 # 32Kb

    try:
        response = s3.get_object(
            Bucket=bucket,
            Key=key,
            Range=f'bytes=0-{CHUNK_BYTES -1}'
        )
        chunk = response['Body'].read().decode('latin-1')
    
    except Exception as e:
        logger.error(f"Error leyendo chunk ARDEF de s3://{bucket}/{key}: {e}")
        return fecha_default
    
    versions = []

    for line in chunk.split('\n'):
        line = line.rstrip('\r\n')

        # La linea debe tener al menos 67 caracteres para contener todos los campos
        if len(line) < 67:
            continue

        if line[0:8] == 'AAACTRNG' and line[10:17] == 'AEPACRN':
            header_date = line[23:31]
            version_number = line[63:67]
            versions.append((version_number, header_date))
            logger.info(
                f"Header ARDEF encontrado | "
                f"version={version_number} | date={header_date}"
            )

    if not versions:
        logger.warning(
            f" No se encontró header ARDEF en los primeros {CHUNK_BYTES // 1024}KB | "
            f"key={key} | usando fecha actual como fallback"
        )
        return fecha_default
        
    # Misma lógica que vi_interpreter: versión más alta se registra
    _, ultimate_date = max(
        versions, 
        key=lambda x: int(x[0]) if str(x[0]).isdigit() else -1,
    )

    try:
        fecha = datetime.strptime(str(ultimate_date), "%Y%m%d").strftime("%Y-%m-%d")
        logger.info(f" Fecha ARDEF extraída: {ultimate_date} -> {fecha}")
        return fecha
    except ValueError:
        logger.warning(f" Fecha ARDEF inválida: '{ultimate_date}' | usando fecha actual")
        return fecha_default
    
# =============================================================================
# DETECCIÓN DE FECHA — ARCHIVOS MASTERCARD IPM (len-prefixed)
# =============================================================================    

# DE spec inline: idéntico a Parameters().getdataelements() de mc_interpreter_handler.
# Solo necesitamos hasta DE48 (DE24 = function_code, DE48 = PDS blob con file_idn).
_MC_DE_SPEC: Dict[int, Dict] = {
    1:   {"fixed": True,  "length": 8},     
    2:   {"fixed": False, "length": 2},     
    3:   {"fixed": True,  "length": 6}, 
    4:   {"fixed": True,  "length": 12},    
    5:   {"fixed": True,  "length": 12},    
    6:   {"fixed": True,  "length": 12},
    9:   {"fixed": True,  "length": 8},     
    10:  {"fixed": True,  "length": 8},     
    12:  {"fixed": True,  "length": 12},
    14:  {"fixed": True,  "length": 4},     
    22:  {"fixed": True,  "length": 12},    
    23:  {"fixed": True,  "length": 3},
    24:  {"fixed": True,  "length": 3},   # function_code (3 bytes: "695", "697", …)
    25:  {"fixed": True,  "length": 4},     
    26:  {"fixed": True,  "length": 4},     
    30:  {"fixed": True,  "length": 24},
    31:  {"fixed": False, "length": 2},     
    32:  {"fixed": False, "length": 2},     
    33:  {"fixed": False, "length": 2},
    37:  {"fixed": True,  "length": 12},    
    38:  {"fixed": True,  "length": 6},     
    40:  {"fixed": True,  "length": 3},
    41:  {"fixed": True,  "length": 8},     
    42:  {"fixed": True,  "length": 15},    
    43:  {"fixed": False, "length": 2},
    48:  {"fixed": False, "length": 3},   # PDS blob (variable, prefijo 3 dígitos)
}


def _mc_to_bool(val) -> bool:
    """
    Convierte un valor DynamoDB (bool/int/str) a bool.

    Args:
        val: Valor a convertir — puede ser None, bool, o cualquier tipo
            convertible a string (ej. "true", "1", "y", "yes", "t").

    Returns:
        True si val es bool True, o su representación string (lowercase,
        strip) está en {'true','1','y','yes','t'}; False en cualquier otro
        caso, incluido None.

    Ejemplo:
        _mc_to_bool("TRUE")  # True
        _mc_to_bool(None)    # False
    """
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ('true', '1', 'y', 'yes', 't')
 
 
def _mc_decode_digits(raw: bytes, is_ebcdic: bool) -> str:
    """
    Decodifica bytes de dígitos a string.
    ASCII  : bytes 0x30-0x39 → '0'-'9'
    EBCDIC : bytes 0xF0-0xF9 → '0'-'9'

    Args:
        raw: Bytes crudos a decodificar (dígitos numéricos).
        is_ebcdic: True si los bytes están en EBCDIC, False si son ASCII/latin-1.

    Returns:
        String decodificado; los bytes EBCDIC fuera de rango 0xF0-0xF9 se
        representan como '?'.

    Ejemplo:
        _mc_decode_digits(b'\xf1\xf2\xf3', is_ebcdic=True)  # "123"
    """
    if is_ebcdic:
        return ''.join(
            chr(ord('0') + (b - 0xF0)) if 0xF0 <= b <= 0xF9 else '?'
            for b in raw
        )
    return raw.decode('latin-1', errors='replace')


def _mc_unblock_chunk(data: bytes, payload_size: int = 1012, sep_size: int = 2) -> bytes:
    """
    Desbloquea un chunk en formato 1014 bytes/bloque:
        [1012 bytes payload][2 bytes separador] …
    Replica unblock_1014() de mc_interpreter_handler sobre bytes ya cargados.
    A diferencia de _mc_unblock_full, siempre descarta 2 bytes de separador
    sin verificar que sean válidos — ver decisions.md → "Por qué el router
    extrae la fecha MC descargando el archivo completo" para el problema que
    esto causaba y por qué extraer_fecha_mc() usa _mc_unblock_full en su
    lugar.

    Args:
        data: Bytes del archivo bloqueado.
        payload_size: Tamaño del payload por bloque (default: 1012).
        sep_size: Tamaño del separador por bloque (default: 2).

    Returns:
        Bytes desbloqueados (payloads concatenados, sin separadores).

    Ejemplo:
        _mc_unblock_chunk(data)
    """
    out = bytearray()
    pos = 0
    while pos < len(data):
        out.extend(data[pos : pos + payload_size])
        pos += payload_size + sep_size
    return bytes(out)


def _mc_unblock_full(data: bytes, payload_size: int = 1012, sep_size: int = 2) -> bytes:
    """
    Desbloquea un archivo IPM bloqueado completo usando la misma lógica de
    unblock_1014() del mc_interpreter_handler, incluyendo valid_seps pushback.

    A diferencia de _mc_unblock_chunk (que siempre salta 2 bytes), esta función
    verifica que cada separador sea válido. Si no lo es, hace pushback: los 2
    bytes quedan como parte del payload del siguiente bloque, evitando que el
    stream quede desalineado en archivos con separadores no estándar.

    Separadores válidos: b"\x40\x40" (EBCDIC space), b"\x20\x20" (ASCII space),
    b"\x00\x00" (null), b"" (vacío — no ocurre en archivos reales pero lo acepta
    el interpreter original).

    Args:
        data: Bytes del archivo bloqueado completo.
        payload_size: Tamaño del payload por bloque (default: 1012).
        sep_size: Tamaño del separador por bloque (default: 2).

    Returns:
        Bytes desbloqueados (payloads concatenados, sin separadores).

    Ejemplo:
        _mc_unblock_full(data)
    """
    valid_seps = (b"\x40\x40", b"\x20\x20", b"\x00\x00", b"")
    stream = io.BytesIO(data)
    out    = bytearray()
    while True:
        chunk = stream.read(payload_size)
        if chunk:
            out.extend(chunk)
        if len(chunk) < payload_size:
            break
        sep = stream.read(sep_size)
        if sep not in valid_seps:
            stream.seek(stream.tell() - len(sep))
    return bytes(out)
 
 
def _mc_parse_bitmap_fields(bitmap: bytes) -> List[int]:
    """
    Devuelve la lista de DEs presentes según el bitmap.
    Bit más significativo (MSB) primero → campo 1, 2, … 128.

    Args:
        bitmap: Bytes del bitmap primario (8 bytes) o primario+secundario (16
            bytes) de un mensaje ISO-8583.

    Returns:
        Lista de números de DE presentes (1-indexados), en orden ascendente.

    Ejemplo:
        _mc_parse_bitmap_fields(bitmap_bytes)  # [2, 3, 4, ...]
    """
    fields: List[int] = []
    for i, byte in enumerate(bitmap):
        for bit in range(8):
            if byte & (1 << (7 - bit)):
                fields.append(i * 8 + bit + 1)
    return fields


def _mc_decode_de48(raw: bytes, is_ebcdic: bool) -> Optional[str]:
    """
    Decodifica los bytes crudos de DE48 a string para parseo de PDS tags.
    ASCII  → latin-1
    EBCDIC → cp500

    Args:
        raw: Bytes crudos del DE48.
        is_ebcdic: True si el mensaje está en EBCDIC.

    Returns:
        String decodificado, o None si la decodificación falla.

    Ejemplo:
        _mc_decode_de48(raw_bytes, is_ebcdic=True)
    """
    try:
        return raw.decode('cp500' if is_ebcdic else 'latin-1', errors='replace')
    except Exception:
        return None
    

def _mc_extract_pds(pds_blob: str, target_tag: str) -> Optional[str]:
    """
    Extrae el valor de un tag PDS del blob DE48.
    Replica extract_pds_value_48_105() de mc_interpreter_handler.

    Estructura PDS: [4 chars tag][3 chars longitud][datos …]

    Args:
        pds_blob: String decodificado del DE48 (secuencia de sub-elementos
            PDS en formato TLV).
        target_tag: Tag PDS a buscar, ej. "0105".

    Returns:
        Valor del tag encontrado, o None si no está presente o el blob está
        corrupto (longitud declarada excede el blob).

    Ejemplo:
        _mc_extract_pds(pds_blob, "0105")  # file_idn
    """
    if not pds_blob:
        return None
    s = pds_blob
    i = 0
    n = len(s)
    while i + 7 <= n:
        tag = s[i:i + 4]
        try:
            ln = int(s[i + 4:i + 7])
        except ValueError:
            return None
        start = i + 7
        end   = start + ln
        if end > n:
            return None
        val = s[start:end]
        if tag == target_tag:
            return val
        i = end
    return None


def _mc_try_parse_1644_695(payload: bytes, is_ebcdic: bool) -> Optional[str]:
    """
    Para un payload de MTI 1644, intenta extraer file_dt si el mensaje es el
    trailer (function_code == "695").

    Replica la lógica de add_headers_fields_697() en mc_interpreter_handler:
      1. Separar bitmap + body (mismo que split_mti_bitmap_body)
      2. Parsear DEs del body hasta DE48 siguiendo _MC_DE_SPEC
      3. DE24 → function_code → confirmar que sea "695"
      4. DE48 → decodificar → PDS tag "0105" → file_idn
      5. Retornar file_idn[3:9]  (= file_dt, formato YYMMDD)

    Args:
        payload: Bytes del mensaje completo (MTI de 4 bytes + bitmap + body).
        is_ebcdic: True si el mensaje está en EBCDIC.

    Returns:
        file_dt en formato YYMMDD (6 caracteres), o None si el payload es
        demasiado corto, no es function_code=695, o no se pudo extraer
        file_idn del DE48.

    Ejemplo:
        _mc_try_parse_1644_695(payload, is_ebcdic=True)  # "260103"
    """
    # Separar bitmap y body (MTI ya conocido = 4 bytes del inicio)
    if len(payload) < 12:
        return None
 
    primary = payload[4:12]
    has_sec = bool(primary[0] & 0x80)
 
    if has_sec:
        if len(payload) < 20:
            return None
        bitmap = payload[4:20]
        body   = payload[20:]
    else:
        bitmap = payload[4:12]
        body   = payload[12:]
 
    fields = _mc_parse_bitmap_fields(bitmap)
 
    # Recorrer DEs hasta DE48 para extraer DE24 (function_code) y DE48 (PDS blob)
    de24_val: Optional[str]   = None
    de48_raw: Optional[bytes] = None
    pos = 0
 
    for de in sorted(f for f in fields if 2 <= f <= 48):
        cfg = _MC_DE_SPEC.get(de)
        if cfg is None:
            break  # campo fuera del spec conocido → detener
 
        if cfg["fixed"]:
            ln = cfg["length"]
            if pos + ln > len(body):
                break
            raw = body[pos : pos + ln]
            pos += ln
        else:
            ld = cfg["length"]
            if pos + ld > len(body):
                break
            raw_len_bytes = body[pos : pos + ld]
            pos += ld
            try:
                ln = int(_mc_decode_digits(raw_len_bytes, is_ebcdic).strip())
            except ValueError:
                break
            if pos + ln > len(body):
                break
            raw = body[pos : pos + ln]
            pos += ln
 
        if de == 24:
            de24_val = _mc_decode_digits(raw, is_ebcdic).strip()
        elif de == 48:
            de48_raw = raw
            break  # ya tenemos todo lo necesario
 
    # Confirmar que es el trailer (function_code = "695")
    if de24_val != "695":
        return None
    if de48_raw is None:
        return None
 
    # Decodificar DE48 → PDS "0105" → file_idn → file_idn[3:9]
    de48_str = _mc_decode_de48(de48_raw, is_ebcdic)
    if de48_str is None:
        return None
 
    file_idn = _mc_extract_pds(de48_str, "0105")
    if not file_idn or len(file_idn) < 9:
        return None
 
    return file_idn[3:9]   # file_dt = file_idn[3:9]  (YYMMDD)


def _mc_scan_for_695(data: bytes) -> Optional[str]:
    """
    Escanea un buffer de bytes buscando el PRIMER mensaje MTI 1644 con
    function_code 695 (trailer de archivo).

    Se detiene en cuanto encuentra el primer trailer válido: todos los trailers
    de un mismo archivo comparten el mismo file_dt, por lo que no tiene sentido
    seguir escaneando si ya se encontró uno.

    Estrategia de alineación:
      El buffer puede comenzar a mitad de un mensaje (leemos desde el final del
      archivo), por lo que buscamos linealmente posiciones donde:
        - Los 4 bytes formen un msg_len plausible (10 … 65535)
        - Los 4 bytes siguientes sean dígitos ASCII o EBCDIC (MTI válido)
      Una vez en un límite válido, avanzamos de mensaje en mensaje con el
      prefijo de longitud.

    Args:
        data: Bytes del archivo (desbloqueado si aplica) a escanear.

    Returns:
        file_dt (YYMMDD) del primer trailer 695 encontrado, o None si no se
        encontró ninguno.

    Ejemplo:
        _mc_scan_for_695(data)  # "260103" o None
    """
    n   = len(data)
    pos = 0
 
    while pos + 8 < n:
        # Intentar leer prefijo de longitud
        try:
            msg_len = struct.unpack(">i", data[pos : pos + 4])[0]
        except Exception:
            pos += 1
            continue
 
        # Filtro 1: longitud plausible para un mensaje IPM (10 bytes … 64 KB)
        if not (10 <= msg_len <= 65535):
            pos += 1
            continue
 
        end_pos = pos + 4 + msg_len
        if end_pos > n:
            pos += 1
            continue
 
        payload   = data[pos + 4 : end_pos]
        mti_bytes = payload[:4]
 
        # Filtro 2: los 4 bytes del MTI deben ser dígitos ASCII o EBCDIC
        is_ascii  = all(0x30 <= b <= 0x39 for b in mti_bytes)
        is_ebcdic = all(0xF0 <= b <= 0xF9 for b in mti_bytes)
 
        if not (is_ascii or is_ebcdic):
            pos += 1
            continue
 
        mti_str = (
            ''.join(str(b - 0xF0) for b in mti_bytes)
            if is_ebcdic else
            mti_bytes.decode('latin-1')
        )
 
        if mti_str == "1644":
            file_dt = _mc_try_parse_1644_695(payload, is_ebcdic)
            if file_dt is not None:
                # Primer trailer encontrado → retornar inmediatamente.
                # Todos los trailers del archivo comparten el mismo file_dt,
                # no tiene sentido seguir escaneando.
                logger.info(f"  MC: primer 695 encontrado | file_dt={file_dt!r} | pos={pos}")
                return file_dt
 
        # Avanzar al siguiente mensaje
        pos = end_pos
 
    return None


def extraer_fecha_mc(
    bucket: str,
    key: str,
    file_block: bool = False,
    interpreter_fix: bool = True,
) -> str:
    """
    Extrae file_dt del PRIMER trailer 1644/695 de un archivo Mastercard IPM.

    Replica la lógica de add_headers_fields_697() en mc_interpreter_handler:
      DE48 del mensaje 695 → PDS tag "0105" → file_idn → file_idn[3:9] → YYMMDD

    Estrategia de lectura:
      Descarga el archivo completo en una sola llamada S3 GetObject.

      Para archivos bloqueados (file_block=True):
        Aplica _mc_unblock_full() que replica exactamente unblock_1014() del
        mc_interpreter_handler, incluyendo valid_seps pushback. Esto es necesario
        porque archivos con separadores no estándar (distintos de \x40\x40) generan
        desalineación de mensajes si se usa _mc_unblock_chunk() (que siempre salta
        2 bytes sin verificar). La desalineación hace que _mc_scan_for_695() no
        encuentre el trailer 695 aunque exista en el archivo — ver decisions.md →
        "Por qué el router extrae la fecha MC descargando el archivo completo"
        para el detalle completo de esta decisión (revertida desde un esquema de
        lectura en chunks).

    Args:
        bucket: Bucket S3 donde está el archivo.
        key: Key del archivo Mastercard IPM.
        file_block: Si el archivo viene bloqueado en bloques de 1014 bytes
            (de clasificacion['file_block'], patrón DynamoDB).
        interpreter_fix: De clasificacion['interpreter_fix'] (documentado; no
            afecta el escaneo de fecha).

    Returns:
        Fecha en formato "YYYY-MM-DD", o la fecha actual (UTC) como fallback
        si el archivo está vacío, falla la descarga, o no se encuentra ningún
        trailer 695.

    Ejemplo:
        extraer_fecha_mc(bucket, key, file_block=True, interpreter_fix=True)
    """
    fecha_default = datetime.utcnow().strftime("%Y-%m-%d")

    # ── 1. Tamaño del archivo (head_object, sin descarga) ─────────────────
    try:
        head      = s3.head_object(Bucket=bucket, Key=key)
        file_size = head['ContentLength']
    except Exception as e:
        logger.error(f"  MC: error en head_object s3://{bucket}/{key}: {e}")
        return fecha_default

    if file_size == 0:
        logger.warning(f"  MC: archivo vacío | key={key}")
        return fecha_default

    # ── 2. Descarga completa ───────────────────────────────────────────────
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        raw      = response['Body'].read()
    except Exception as e:
        logger.error(f"  MC: error descargando s3://{bucket}/{key}: {e}")
        return fecha_default

    logger.info(
        f"  MC: descargado {len(raw):,} bytes | file_block={file_block} | key={key}"
    )

    # ── 3. Desbloquear si es necesario ────────────────────────────────────
    if file_block:
        data = _mc_unblock_full(raw)
        logger.info(f"  MC: desbloqueado {len(raw):,} → {len(data):,} bytes")
    else:
        data = raw

    # ── 4. Escanear buscando el PRIMER 695 ────────────────────────────────
    file_dt = _mc_scan_for_695(data)

    if file_dt:
        for fmt in ("%y%m%d", "%m%d%y"):
            try:
                fecha = datetime.strptime(file_dt, fmt).strftime("%Y-%m-%d")
                logger.info(
                    f"  MC: fecha extraída={fecha} "
                    f"| file_dt={file_dt!r} | key={key}"
                )
                return fecha
            except ValueError:
                continue
        logger.warning(f"  MC: file_dt no parseable={file_dt!r} | key={key}")
        return fecha_default

    # ── 5. Fallback ────────────────────────────────────────────────────────
    logger.warning(f"  MC: no se encontró trailer 695 | key={key}")

    return fecha_default
    





# =============================================================================
# CLASIFICACIÓN DE ARCHIVOS
# =============================================================================

def cargar_patrones(customer_code: str = None) -> List[Dict]:
    """
    Carga patrones de clasificación desde DynamoDB (tabla file_pattern).
    Filtra por customer_code o 'ALL', ordenados por prioridad.

    Args:
        customer_code: Código de cliente a filtrar (además de los patrones
            'ALL', genéricos para cualquier cliente); si es None, no filtra
            por cliente.

    Returns:
        Lista de patrones activos (is_active=1), ordenados por priority
        ascendente, o lista vacía si no hay ninguno o falla la consulta.

    Ejemplo:
        cargar_patrones("EBGR")  # [{'pattern_id': ..., 'file_format': ..., ...}, ...]
    """
    try:
        table    = dynamodb.Table(TABLE_FILE_PATTERN)
        response = table.scan(
            FilterExpression='is_active = :active',
            ExpressionAttributeValues={':active': 1}
        )
        items = response.get('Items', [])
        
        # Paginar si hay más de 1MB de resultados
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='is_active = :active',
                ExpressionAttributeValues={':active': 1},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        if not items:
            logger.warning("No hay patrones activos en DynamoDB")
            return []

        items.sort(key=lambda x: int(x.get('priority', 999)))

        if customer_code:
            items = [p for p in items if p.get('customer_code') in [customer_code, 'ALL']]

        logger.info(f"  {len(items)} patrones cargados para '{customer_code}'")
        return items

    except Exception as e:
        logger.error(f"Error cargando patrones: {e}")
        return []


def clasificar_archivo(filename: str, patrones: List[Dict]) -> Optional[Dict]:
    """
    Aplica los patrones regex en orden de prioridad.
    Retorna la clasificación del primer match, o None.

    Args:
        filename: Nombre del archivo a clasificar.
        patrones: Lista de patrones ya cargados y ordenados por prioridad
            (ver cargar_patrones).

    Returns:
        Dict con brand, direction, file_type, customer_code, pattern_id,
        file_block, interpreter_fix del primer patrón cuyo regex matchea
        filename; o None si ninguno matchea.

    Ejemplo:
        clasificar_archivo("VS.EBGR.TC00.20260103.001.txt", patrones)
        # {'brand': 'VISA', 'direction': 'IN', 'file_type': 'BASEII', ...}
    """
    for patron in patrones:
        regex = patron.get("file_format", "")
        if not regex:
            continue
        try:
            if re.search(regex, filename, re.IGNORECASE):
                logger.info(f"  Match patrón: {patron.get('pattern_id')} ({regex[:50]})")
                return {
                    "brand":         patron.get("brand", "UNKNOWN"),
                    "direction":     patron.get("direction", "UNKNOWN"),
                    "file_type":     patron.get("file_type", "UNKNOWN"),
                    "customer_code": patron.get("customer_code"),
                    "pattern_id":    patron.get("pattern_id"),
                    # Configuración de lectura MC: mismos campos que usa
                    # needs_unblock_for_file / needs_interpreter_fix en mc_interpreter_handler
                    "file_block":      _mc_to_bool(patron.get("file_block",      False)),
                    "interpreter_fix": _mc_to_bool(patron.get("interpreter_fix", True)),
                }
        except re.error as e:
            logger.warning(f"  Regex inválido en patrón {patron.get('pattern_id')}: {e}")

    return None


# =============================================================================
# CONTROL DE DUPLICADOS
# =============================================================================

def verificar_duplicado(file_id: str, content_hash: str) -> Tuple[str, Optional[str]]:
    """
    Verifica si el archivo ya fue procesado, comparando su content_hash
    contra el registro existente en DynamoDB (file_control) por file_id.

    Args:
        file_id: Identificador determinista del archivo (ver generar_file_id).
        content_hash: MD5 del contenido actual del archivo.

    Returns:
        Tupla (estado, file_id_existente):
          ("nuevo", None)            → nunca visto
          ("duplicado", file_id)     → mismo nombre Y mismo contenido → ignorar
          ("version_nueva", file_id) → mismo nombre, distinto contenido → reprocesar
        Ante cualquier error de DynamoDB, retorna ("nuevo", None) como
        fallback conservador.

    Ejemplo:
        verificar_duplicado(file_id, content_hash)  # ("duplicado", file_id)
    """
    try:
        table    = dynamodb.Table(TABLE_FILE_CONTROL)
        response = table.get_item(Key={'file_id': file_id})

        if 'Item' not in response:
            return ("nuevo", None)

        hash_existente = response['Item'].get('content_hash', '')

        if hash_existente == content_hash:
            return ("duplicado", file_id)
        else:
            return ("version_nueva", file_id)

    except Exception as e:
        logger.warning(f"Error verificando duplicado: {e}")
        return ("nuevo", None)


# =============================================================================
# ARCHIVOS SIN MATCH DE PATRÓN (file_pattern)
# =============================================================================

def procesar_archivo_desconocido(
    bucket: str, key: str, event_size: int, client_id: str, filename: str
) -> Dict:
    """
    Maneja un archivo que no matcheó ningún patrón activo de
    `file_pattern` — antes de esta función, ese archivo se descartaba en
    silencio (ver gotcha "lmbd-router: archivo sin match de patrón..." en
    gotchas.md): sin registro en `file_control`, sin moverse de landing,
    solo un `logger.warning` que nadie ve salvo que vaya a buscarlo a
    mano en CloudWatch.

    Reusa exactamente el mismo cálculo de identidad que un archivo
    clasificado (`generar_file_id`/`calcular_content_hash`/
    `verificar_duplicado`/`generar_file_id_unico`) — la identidad de un
    archivo no depende de si matcheó un patrón, así que no hay motivo
    para tener una lógica de deduplicación distinta acá. Diferencias
    reales respecto al flujo normal:
      - `clasificacion` sintética ({'brand': 'UNKNOWN', 'direction':
        'UNKNOWN'}) — FILE_TYPE_MAP/BRAND_ID_MAP resuelven cualquier
        valor no reconocido a 'UNKNOWN' por su propio default.
      - `control_status='UNKNOWN'` directo (nunca pasa por PENDING ni
        por `actualizar_estado()` — no hay ningún pipeline que lo vaya a
        mover de estado).
      - Se archiva a `s3-archive` en vez de dejarse en landing (ver
        `archivar_desconocido()`); la key resultante se persiste como
        `archive_key` (columna top-level, `update_item` no destructivo)
        para no depender solo del retorno del Lambda.
      - Nunca se llama a `start_process()`.

    Args:
        bucket: Bucket S3 de landing.
        key: Key completo del archivo en landing.
        event_size: Tamaño reportado por el evento S3 (fallback si
            `obtener_file_size` no puede confirmar el tamaño real).
        client_id: Código de cliente (primer segmento del path).
        filename: Nombre del archivo (último segmento del path).

    Returns:
        Dict para agregar a `results` — mismo formato que el resto de
        ramas de `lambda_handler()` (`status` en {"SKIPPED", "ERROR"}).

    Ejemplo:
        procesar_archivo_desconocido("landing", "EBGR/raro.dat", 1024, "EBGR", "raro.dat")
        # -> {'file': 'raro.dat', 'status': 'SKIPPED', 'reason': 'No pattern match',
        #     'file_id': '...', 'control_status': 'UNKNOWN', 'archived_key': 'EBGR/originals/UNKNOWN/...'}
    """
    file_id = generar_file_id(client_id, filename)

    content_hash = calcular_content_hash(bucket, key)
    if not content_hash:
        logger.warning("  content_hash vacío → usando file_id como fallback")
        content_hash = file_id

    file_size = obtener_file_size(bucket, key, event_size)
    file_date = datetime.utcnow().strftime("%Y-%m-%d")

    estado_dup, _ = verificar_duplicado(file_id, content_hash)
    if estado_dup == "duplicado":
        return procesar_archivo_duplicado(file_id, bucket, key, client_id, filename, "Duplicate (unknown)")
    elif estado_dup == "version_nueva":
        logger.info("  VERSION NUEVA (desconocido) — generando nuevo file_id")
        file_id = generar_file_id_unico(client_id, filename, content_hash)

    clasificacion_desconocida = {'brand': 'UNKNOWN', 'direction': 'UNKNOWN'}

    if not registrar_archivo(
        file_id=file_id, client_id=client_id, filename=filename,
        bucket=bucket, s3_key=key, file_size=file_size,
        content_hash=content_hash, clasificacion=clasificacion_desconocida,
        file_date=file_date, control_status='UNKNOWN',
        error_message='Sin match de patrón en file_pattern',
    ):
        logger.error("  Falló registro en DynamoDB (desconocido)")
        return {'file': filename, 'status': 'ERROR', 'error': 'DynamoDB failed (unknown)'}

    archived_key = archivar_desconocido(bucket, key, client_id, filename)

    if archived_key:
        # Mismo formato que el flujo normal (ver ASL VI/MC, paso
        # UpdateFileControlArchived): archive_key es un string con el
        # JSON de {archive_key, file_id, status} — no la key sola.
        archive_result_json = json.dumps({
            'status':      'ARCHIVED',
            'file_id':     file_id,
            'archive_key': archived_key,
        })
        try:
            dynamodb.Table(TABLE_FILE_CONTROL).update_item(
                Key={'file_id': file_id},
                UpdateExpression="SET archive_key = :ak",
                ExpressionAttributeValues={':ak': archive_result_json},
            )
        except Exception as e:
            logger.warning(f"  No se pudo persistir archive_key para {file_id}: {e}")

    return {
        'file':          filename,
        'status':        'SKIPPED',
        'reason':        'No pattern match',
        'file_id':       file_id,
        'control_status': 'UNKNOWN',
        'archived_key':  archived_key,
    }


def _compress_and_save_to_tmp(source_bucket: str, source_key: str, filename: str, tmp_path: str) -> int:
    """
    Lee el archivo de S3 en chunks y lo escribe comprimido en /tmp — port
    literal de `_compress_and_save_to_tmp()` de `lmbd-archive-file`
    (mismo patrón, mismo default de chunk vía `COMPRESS_CHUNK_SIZE_MB`).
    Nunca tiene más de `COMPRESS_CHUNK_BYTES` en RAM al mismo tiempo.

    Args:
        source_bucket: Bucket origen (landing).
        source_key: Key del archivo original dentro del bucket origen.
        filename: Nombre con el que se registra la entrada dentro del ZIP.
        tmp_path: Ruta local en /tmp donde se escribe el ZIP resultante.

    Returns:
        Tamaño del ZIP resultante en bytes.

    Ejemplo:
        _compress_and_save_to_tmp("itx-landing-dev", "EBGR/raro.dat",
            "raro.dat", "/tmp/EBGR_raro.dat.zip")  # -> 20480
    """
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    body = response['Body']
    file_size = response.get('ContentLength', 0)

    logger.info(f"  Comprimiendo s3://{source_bucket}/{source_key} ({file_size / 1024 / 1024:.1f}MB)")

    with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        with zf.open(filename, 'w') as zip_entry:
            while True:
                chunk = body.read(COMPRESS_CHUNK_BYTES)
                if not chunk:
                    break
                zip_entry.write(chunk)

    zip_size = os.path.getsize(tmp_path)
    logger.info(f"  ZIP: {zip_size / 1024 / 1024:.1f}MB")
    return zip_size


def _upload_zip_to_s3(tmp_path: str, dest_bucket: str, dest_key: str, zip_size: int) -> None:
    """
    Sube el ZIP desde /tmp a S3 — port literal de `_upload_zip_to_s3()` de
    `lmbd-archive-file`: `put_object` simple para archivos < 100MB,
    multipart upload (abortado limpio si falla a mitad de camino) para
    archivos más grandes.

    Args:
        tmp_path: Ruta local del ZIP a subir.
        dest_bucket: Bucket destino (archive).
        dest_key: Key destino dentro del bucket de archive.
        zip_size: Tamaño del ZIP en bytes, decide la estrategia de subida.

    Returns:
        None. Relanza la excepción original si el multipart upload falla
        (después de abortarlo, para no dejar partes huérfanas en S3).

    Ejemplo:
        _upload_zip_to_s3("/tmp/EBGR_raro.dat.zip", "itl-...-s3-archive",
            "EBGR/originals/UNKNOWN/2026/08/raro.dat.zip", 20480)
    """
    if zip_size < MULTIPART_THRESHOLD:
        with open(tmp_path, 'rb') as f:
            s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=f, ContentType='application/zip')
        return

    mpu = s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key, ContentType='application/zip')
    upload_id = mpu['UploadId']
    parts = []
    part_num = 1
    try:
        with open(tmp_path, 'rb') as f:
            while True:
                chunk = f.read(MULTIPART_THRESHOLD)
                if not chunk:
                    break
                response = s3.upload_part(
                    Bucket=dest_bucket, Key=dest_key, UploadId=upload_id,
                    PartNumber=part_num, Body=chunk,
                )
                parts.append({'PartNumber': part_num, 'ETag': response['ETag']})
                part_num += 1
        s3.complete_multipart_upload(
            Bucket=dest_bucket, Key=dest_key, UploadId=upload_id,
            MultipartUpload={'Parts': parts},
        )
    except Exception:
        logger.error("Multipart upload falló — abortando")
        s3.abort_multipart_upload(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id)
        raise


def _archivar_bajo_prefijo(bucket: str, key: str, client_id: str, filename: str, prefijo: str) -> Optional[str]:
    """
    Mueve a s3-archive un archivo que el router no va a mandar al
    pipeline normal — reusada tanto para archivos sin match de patrón
    (`prefijo="UNKNOWN"`, ver gotcha "lmbd-router: archivo sin match de
    patrón..." en gotchas.md) como para duplicados de cualquier archivo,
    clasificado o no (`prefijo="DUPLICATE"`, ver `procesar_archivo_duplicado()`).
    Sin esto, ambos casos dejaban el archivo indefinidamente en landing.

    Estructura de destino, en paralelo a la de `lmbd-archive-file`
    (`{client_id}/originals/{brand}/{file_type}/{year}/{month}/{filename}.zip`)
    pero bajo `{prefijo}` en vez de `{brand}/{file_type}` — lo distingue
    de inmediato de un archivo archivado por el flujo normal:

      {client_id}/originals/{prefijo}/{year}/{month}/{filename}.zip

    Comprime a .zip antes de subir — mismo patrón de streaming que
    `lmbd-archive-file` (`_compress_and_save_to_tmp`/`_upload_zip_to_s3`,
    portados literal más arriba), para no cargar el archivo completo en
    memoria. Verifica que el ZIP exista en destino (`head_object`) antes
    de borrar el original de landing — si la verificación falla, el
    archivo original queda intacto en landing en vez de perderse. `/tmp`
    se limpia siempre, éxito o fallo (`finally`). `year`/`month` usan la
    fecha de HOY (no hay fecha de negocio confiable en ninguno de los 2
    casos — un archivo sin clasificar no tiene fecha extraíble, y un
    duplicado usa la fecha de ESTA subida, no la original).

    Args:
        bucket: Bucket S3 de landing (origen).
        key: Key completo del archivo en landing.
        client_id: Código de cliente (primer segmento del path).
        filename: Nombre del archivo (último segmento del path).
        prefijo: Segmento que reemplaza a `{brand}/{file_type}` en la
            ruta de destino — "UNKNOWN" o "DUPLICATE".

    Returns:
        El S3 key de destino (.zip) en `ARCHIVE_BUCKET` si el archivado
        tuvo éxito, `None` si `ARCHIVE_BUCKET` no está configurado o si
        algo falló (se loguea el error, nunca se relanza — dejar el
        archivo en landing sigue siendo mejor que tumbar el resto del
        batch).

    Ejemplo:
        _archivar_bajo_prefijo("landing-bucket", "EBGR/raro.dat", "EBGR", "raro.dat", "UNKNOWN")
        # -> "EBGR/originals/UNKNOWN/2026/08/raro.dat.zip"
    """
    if not ARCHIVE_BUCKET:
        logger.warning(f"  S3_BUCKET_ARCHIVE no configurado — archivo ({prefijo}) queda en landing")
        return None

    now = datetime.utcnow()
    zip_filename = filename if filename.lower().endswith('.zip') else f"{filename}.zip"
    dest_key = f"{client_id}/originals/{prefijo}/{now.strftime('%Y')}/{now.strftime('%m')}/{zip_filename}"
    tmp_path = f"/tmp/{client_id}_{zip_filename}"

    try:
        zip_size = _compress_and_save_to_tmp(bucket, key, filename, tmp_path)
        _upload_zip_to_s3(tmp_path, ARCHIVE_BUCKET, dest_key, zip_size)

        try:
            s3.head_object(Bucket=ARCHIVE_BUCKET, Key=dest_key)
        except Exception:
            logger.error(f"Verificación de subida falló — {filename} queda en landing sin borrar")
            return None

        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"  Archivado ({prefijo}) → s3://{ARCHIVE_BUCKET}/{dest_key}")
        return dest_key
    except Exception as e:
        logger.error(f"Error archivando ({prefijo}): {e}")
        return None
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception as e:
                logger.warning(f"No se pudo limpiar {tmp_path}: {e}")


def archivar_desconocido(bucket: str, key: str, client_id: str, filename: str) -> Optional[str]:
    return _archivar_bajo_prefijo(bucket, key, client_id, filename, "UNKNOWN")


def procesar_archivo_duplicado(
    file_id: str, bucket: str, key: str, client_id: str, filename: str, motivo: str
) -> Dict:
    """
    Maneja un archivo detectado como duplicado exacto (mismo file_id Y
    mismo content_hash que un registro ya existente en file_control) —
    aplica tanto al duplicado de un archivo YA CLASIFICADO (detectado en
    `lambda_handler()`) como al de uno sin clasificar (detectado en
    `procesar_archivo_desconocido()`), de ahí que reciba `motivo` como
    texto libre para distinguir el origen en logs/resultados.

    Deliberadamente NO usa `registrar_archivo()` (que hace `put_item` —
    reemplazaría el registro completo). El `file_id` de un duplicado es
    el MISMO que el del archivo procesado la primera vez, y ese registro
    ya tiene el `control_status` real de esa corrida (SUCCESS, FAILED, lo
    que haya sido) — sobreescribirlo perdería esa historia. En su lugar,
    `update_item` con `list_append` AGREGA un evento a
    `duplicate_uploads` sin tocar ningún otro campo (mismo principio que
    "nunca REMOVE atributos" de otras tablas del proyecto — acá el
    equivalente es "nunca overwrite un registro con historia real").

    Efecto en S3: el duplicado se archiva a `.../originals/DUPLICATE/...`
    (ver `_archivar_bajo_prefijo()`) — landing queda limpio igual que en
    el resto de los casos que maneja el router.

    Args:
        file_id: file_id ya existente en file_control (mismo que el
            archivo original — NO se genera uno nuevo).
        bucket: Bucket S3 de landing.
        key: Key completo del archivo duplicado en landing.
        client_id: Código de cliente.
        filename: Nombre del archivo.
        motivo: Texto para logs/resultados — "Duplicate" o
            "Duplicate (unknown)" según el caso.

    Returns:
        Dict para agregar a `results` (`status: "SKIPPED"`).

    Ejemplo:
        procesar_archivo_duplicado(file_id, bucket, key, "EBGR", "archivo.txt", "Duplicate")
    """
    archived_key = _archivar_bajo_prefijo(bucket, key, client_id, filename, "DUPLICATE")

    try:
        table = dynamodb.Table(TABLE_FILE_CONTROL)
        table.update_item(
            Key={'file_id': file_id},
            UpdateExpression=(
                "SET duplicate_uploads = list_append("
                "if_not_exists(duplicate_uploads, :empty), :new_upload)"
            ),
            ExpressionAttributeValues={
                ':empty': [],
                ':new_upload': [{
                    'detected_at':   datetime.utcnow().isoformat(),
                    'landing_path':  f"s3://{bucket}/{key}",
                    'archived_key':  archived_key or '',
                }],
            },
        )
    except Exception as e:
        logger.warning(f"  No se pudo registrar duplicate_uploads para {file_id}: {e}")

    logger.info(f"  DUPLICADO ({motivo}): {file_id}")
    return {
        'file':         filename,
        'status':       'SKIPPED',
        'reason':       motivo,
        'file_id':      file_id,
        'archived_key': archived_key,
    }


# =============================================================================
# REGISTRO EN DYNAMODB
# =============================================================================

def registrar_archivo(
    file_id: str, client_id: str, filename: str,
    bucket: str, s3_key: str, file_size: int,
    content_hash: str, clasificacion: Dict, file_date: str,
    control_status: str = 'PENDING', error_message: Optional[str] = None,
) -> bool:
    """
    Crea el registro inicial del archivo en DynamoDB (tabla file_control).
    Estado inicial: PENDING (o el que se pase en `control_status` — ver
    caso de archivos sin match de patrón en `lambda_handler()`, que se
    registran directo en 'UNKNOWN' porque nunca van a pasar por
    `actualizar_estado()`).

    Args:
        file_id: Identificador del archivo (llave de partición).
        client_id: Código de cliente.
        filename: Nombre del archivo original.
        bucket: Bucket S3 de landing.
        s3_key: Key del archivo en landing.
        file_size: Tamaño del archivo en bytes.
        content_hash: MD5 del contenido del archivo.
        clasificacion: Dict de clasificación (de clasificar_archivo), usado
            para derivar brand_id y file_type. Para archivos sin match,
            pasar {'brand': 'UNKNOWN', 'direction': 'UNKNOWN'} — los mapas
            FILE_TYPE_MAP/BRAND_ID_MAP resuelven cualquier valor no
            reconocido a 'UNKNOWN' vía su propio default.
        file_date: Fecha de negocio del archivo, en formato "YYYY-MM-DD".
        control_status: Estado inicial del registro. 'PENDING' (default)
            para el flujo normal — el pipeline lo actualiza después vía
            `actualizar_estado()`. Un valor terminal (ej. 'UNKNOWN') para
            archivos que nunca van a entrar al pipeline.
        error_message: Motivo a registrar cuando `control_status` ya es
            terminal (ej. "Sin match de patrón en file_pattern") — evita
            tener que loguear el motivo solo en CloudWatch.

    Returns:
        True si el registro se creó exitosamente, False si falló (se loguea
        el error, no se relanza).

    Ejemplo:
        registrar_archivo(file_id, "EBGR", filename, bucket, key, 104857600,
                           content_hash, clasificacion, "2026-01-03")
    """
    try:
        table = dynamodb.Table(TABLE_FILE_CONTROL)
        direction = clasificacion['direction'].upper()
        brand = clasificacion['brand'].upper()

        file_type = FILE_TYPE_MAP.get(direction, 'UNKNOWN')
        brand_id = BRAND_ID_MAP.get(brand, 'UNKNOWN')

        registro = {
            'file_id':              file_id,
            'client_id':            client_id,
            'landing_file_name':    filename,
            'file_path':            f"s3://{bucket}/{s3_key}",
            'file_size':            file_size,
            'content_hash':         content_hash,
            'brand_id':             brand_id,
            'file_type':            file_type,
            'file_processing_date': file_date,
            'detected_at':          datetime.utcnow().isoformat(),
            'control_status':       control_status,
            'process_start_ts':     None,
            'process_finish_ts':    None,
            'error_message':        error_message,
        }

        table.put_item(Item=registro)
        logger.info(f"  Archivo registrado → file_id: {file_id} (control_status={control_status})")
        return True

    except Exception as e:
        logger.error(f"Error registrando archivo en DynamoDB: {e}")
        return False


def actualizar_estado(file_id: str, estado: str, error: str = None):
    """
    Actualiza el estado de procesamiento en DynamoDB.
    Estados: PENDING → PROCESSING → COMPLETED | FAILED

    Args:
        file_id: Identificador del archivo (llave de partición).
        estado: Nuevo estado ("PROCESSING", "COMPLETED", "FAILED", etc. — se
            normaliza a mayúscula).
        error: Mensaje de error a registrar (solo relevante si estado es
            "FAILED"); se trunca a 500 caracteres.

    Returns:
        None. Cualquier error de DynamoDB se loguea, no se relanza.

    Ejemplo:
        actualizar_estado(file_id, "PROCESSING")
        actualizar_estado(file_id, "FAILED", "Timeout en Step Function")
    """
    try:
        table   = dynamodb.Table(TABLE_FILE_CONTROL)
        now     = datetime.utcnow().isoformat()
        estado  = estado.upper()

        update_expr = "SET control_status = :status"
        expr_values = {':status': estado}

        if estado == 'PROCESSING':
            update_expr += ", process_start_ts = :ts"
            expr_values[':ts'] = now
        elif estado in ('COMPLETED', 'FAILED'):
            update_expr += ", process_finish_ts = :ts"
            expr_values[':ts'] = now

        if error:
            update_expr += ", error_message = :err"
            expr_values[':err'] = str(error)[:500]

        table.update_item(
            Key={'file_id': file_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values
        )
        logger.info(f"  Estado → {estado} (file_id: {file_id})")

    except Exception as e:
        logger.error(f"Error actualizando estado: {e}")


# =============================================================================
# INICIO DE STEP FUNCTIONS
# =============================================================================

def start_process(
    client_id: str, file_id: str, filename: str,
    bucket: str, s3_key: str, clasificacion: Dict,
    file_date: str, content_hash: str
) -> str:
    """
    Inicia la ejecución de los procesos con toda la metadata del archivo,
    despachando según direction/brand: ARDEF/IAR invocan una Lambda directa
    (async, sin Step Functions — ver decisions.md → "Por qué ARDEF e IAR no
    usan Step Functions"), VISA/MASTERCARD inician la Step Function
    correspondiente.

    El content_hash se usa downstream para nombrar los archivos Parquet.
    NUNCA debe ser vacío: si calcular_content_hash falla, el caller
    debe pasar file_id como fallback antes de llegar aquí.

    Args:
        client_id: Código de cliente.
        file_id: Identificador del archivo.
        filename: Nombre del archivo original.
        bucket: Bucket S3 de landing.
        s3_key: Key del archivo en landing.
        clasificacion: Dict de clasificación (de clasificar_archivo).
        file_date: Fecha de negocio del archivo, en formato "YYYY-MM-DD".
        content_hash: MD5 del contenido del archivo (nunca vacío).

    Returns:
        Referencia al proceso iniciado: el executionArn de la Step Function
        (VISA/MASTERCARD), o "LAMBDA:{nombre}:{request_id}" (ARDEF/IAR).

    Raises:
        ValueError: si la combinación brand/direction no tiene un proceso
            configurado.

    Ejemplo:
        start_process("EBGR", file_id, filename, bucket, key, clasificacion,
                      "2026-01-03", content_hash)
    """
    direction = clasificacion['direction'].upper()
    brand = clasificacion['brand'].upper()

    file_type = FILE_TYPE_MAP.get(direction, 'UNKNOWN')
    brand_id = BRAND_ID_MAP.get(brand, 'UNKNOWN')

    variables_input = {
        'client_id':      client_id,
        'file_id':        file_id,
        'filename':       filename,
        's3_key_landing': s3_key,
        'bucket_landing': bucket,
        'brand':          brand,
        'brand_id':       brand_id,
        'file_type':      file_type,
        'file_date':      file_date,
        'content_hash':   content_hash,   # Nunca vacío — ver fallback en handler
    }

    execution_name = (
        f"{client_id}-{file_id[:8]}-"
        f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    )
     
    process_reference = None
       
    if direction == 'IAR':
        response = lambda_client.invoke(
            FunctionName=MASTERCARD_IAR_FUNCTION_NAME,
            InvocationType='Event',
            Payload=json.dumps(variables_input).encode()
        )
        request_id = response['ResponseMetadata']['RequestId']
        process_reference = (f"LAMBDA:{MASTERCARD_IAR_FUNCTION_NAME}:{request_id}")
        
    elif direction == 'ARDEF':
        response = lambda_client.invoke(
            FunctionName=VISA_ARDEF_FUNCTION_NAME,
            InvocationType='Event',
            Payload=json.dumps(variables_input).encode()
        )
        request_id = response['ResponseMetadata']['RequestId']
        process_reference = (f"LAMBDA:{VISA_ARDEF_FUNCTION_NAME}:{request_id}")
        
    elif brand == 'VISA':
        response = sfn.start_execution(
            stateMachineArn=STEP_FUNCTION_VI_ARN,
            name=execution_name,
            input=json.dumps(variables_input)
        )
        process_reference = response['executionArn']
    elif brand == 'MASTERCARD':
        response = sfn.start_execution(
        stateMachineArn=STEP_FUNCTION_MC_ARN,
        name=execution_name,
        input=json.dumps(variables_input)
        )
    else:
        raise ValueError(
            f"No existe proceso configurado para "
            f"brand={brand}, direction={direction}"
        )      

    logger.info(f"Proceso iniciado: {process_reference}")
    return process_reference


# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Punto de entrada de la Lambda lmbd-router. Se dispara por eventos S3
    ObjectCreated en el bucket de landing — un evento puede contener múltiples
    records (batch de S3), procesados independientemente (un fallo en un
    record no aborta los demás).

    Por cada record: extrae client_id/filename del path, delega a itx-unzip
    si es un ZIP, carga y aplica los patrones de clasificación, genera el
    file_id, calcula el content_hash en streaming, extrae la fecha de negocio
    (lógica distinta por IAR/ARDEF/Visa/Mastercard), verifica duplicados
    contra DynamoDB, registra el archivo (estado PENDING → PROCESSING) y
    despacha el procesamiento (start_process).

    Args:
        event: Evento S3 con event['Records'], cada uno con
            record['s3']['bucket']['name'] y record['s3']['object']['key'].
        context: Contexto de ejecución de Lambda (no usado).

    Returns:
        Dict con statusCode=200 y body (JSON) con la lista results — un dict
        por record procesado, con status en {"DELEGATED_TO_UNZIP", "STARTED",
        "SKIPPED", "ERROR"} y detalles según el caso.

    Ejemplo:
        lambda_handler({'Records': [{'s3': {'bucket': {'name': '...'},
                         'object': {'key': 'EBGR/VS.EBGR.TC00.20260103.001.txt'}}}]}, None)
    """
    logger.info("=== ITX Router Lambda ===")
    logger.info(f"Event: {json.dumps(event)}")

    validar_configuracion()

    results = []

    for record in event.get('Records', []):
        bucket   = None
        key      = None
        filename = "unknown"

        try:
            # Extraer datos del evento S3
            bucket     = record['s3']['bucket']['name']
            key        = unquote_plus(record['s3']['object']['key'])
            event_size = record['s3']['object'].get('size', 0)

            logger.info(f"--- Procesando: s3://{bucket}/{key} ({event_size:,} bytes) ---")

            # Validar estructura del path: CLIENT_ID/filename
            parts = key.split('/')
            if len(parts) < 2:
                logger.error(f"Path inválido: {key}")
                results.append({'file': key, 'status': 'ERROR', 'error': 'Invalid path'})
                continue

            client_id = parts[0]
            filename  = parts[-1]

            # Ignorar archivos ocultos y carpetas vacías
            if not filename or filename.startswith('.'):
                logger.info(f"Ignorando: {key}")
                continue

            logger.info(f"  Client: {client_id}, File: {filename}")

            # ── Detectar ZIP → delegar a itx-unzip ───────────────────────
            if _is_zip_file(filename):
                file_date  = _extraer_fecha_de_zip(filename)
                zip_result = _handle_zip(bucket, key, client_id, file_date)
                results.append(zip_result)
                continue
            # ─────────────────────────────────────────────────────────────

            # Cargar patrones. Un cliente sin NINGÚN patrón activo (a
            # diferencia de "tiene patrones pero este archivo no matchea
            # ninguno") ya NO corta como ERROR (2026-08-12) — antes,
            # `cargar_patrones()` vacío mandaba el archivo directo a un
            # status "ERROR" sin registrar nada ni archivar, el mismo
            # problema de fondo que procesar_archivo_desconocido() ya
            # resuelve para el otro caso. `clasificar_archivo(filename, [])`
            # ya devuelve None de forma natural con una lista vacía, así
            # que ambos casos ahora caen en la misma rama "sin match".
            patrones = cargar_patrones(client_id)

            # Clasificar
            clasificacion = clasificar_archivo(filename, patrones)
            if not clasificacion:
                logger.warning(f"  Sin match de patrón: {filename}")
                results.append(procesar_archivo_desconocido(
                    bucket=bucket, key=key, event_size=event_size,
                    client_id=client_id, filename=filename,
                ))
                continue

            logger.info(f"  Clasificado: {clasificacion['brand']} / {clasificacion['direction']}")

            # Generar file_id
            file_id = generar_file_id(client_id, filename)

            # Calcular content_hash en streaming
            content_hash = calcular_content_hash(bucket, key)
            if not content_hash:
                logger.warning("  content_hash vacío → usando file_id como fallback")
                content_hash = file_id

            # Extraer fecha del header       
            file_date = datetime.utcnow().strftime("%Y-%m-%d")  
              
            if clasificacion['direction'] == 'IAR':
                file_date= extraer_fecha_iar(bucket, key)
            elif clasificacion['direction'] == 'ARDEF':
                file_date = extraer_fecha_ardef(bucket, key)
            elif clasificacion['brand'] == 'VISA':
                file_date= extraer_fecha(bucket, key)
            elif clasificacion['brand'] == 'MASTERCARD':
                file_date = extraer_fecha_mc(
                    bucket=bucket,
                    key=key,
                    file_block=clasificacion.get('file_block', False),
                    interpreter_fix=clasificacion.get('interpreter_fix', True),
                )
            
            # Extraer tamaño de archivo                        
            file_size = obtener_file_size(bucket, key, event_size)

            logger.info(f"  file_id: {file_id[:16]}... | date: {file_date} | size: {file_size:,}B")

            # Verificar duplicado
            estado_dup, _ = verificar_duplicado(file_id, content_hash)

            if estado_dup == "duplicado":
                results.append(procesar_archivo_duplicado(file_id, bucket, key, client_id, filename, "Duplicate"))
                continue

            elif estado_dup == "version_nueva":
                logger.info("  VERSION NUEVA — generando nuevo file_id")
                file_id = generar_file_id_unico(client_id, filename, content_hash)
                logger.info(f"  Nuevo file_id: {file_id[:16]}...")

            # Registrar en DynamoDB
            if not registrar_archivo(
                file_id=file_id, client_id=client_id, filename=filename,
                bucket=bucket, s3_key=key, file_size=file_size,
                content_hash=content_hash, clasificacion=clasificacion,
                file_date=file_date
            ):
                logger.error("  Falló registro en DynamoDB")
                results.append({'file': filename, 'status': 'ERROR', 'error': 'DynamoDB failed'})
                continue

            # Iniciar procesos (Step Functions o Lambdas según clasificación)
            actualizar_estado(file_id, 'PROCESSING')

            try:
                execution_id = start_process(
                    client_id=client_id, file_id=file_id, filename=filename,
                    bucket=bucket, s3_key=key, clasificacion=clasificacion,
                    file_date=file_date, content_hash=content_hash
                )
                logger.info(f"  Procesamiento iniciado: {execution_id}")
                results.append({
                    'file':          filename,
                    'status':        'STARTED',
                    'file_id':       file_id,
                    'execution_id':  execution_id
                })

            except Exception as e:
                logger.error(f"  Error iniciando Step Functions: {e}")
                actualizar_estado(file_id, 'FAILED', str(e))
                results.append({'file': filename, 'status': 'ERROR', 'error': str(e)})
                continue  # no raise — procesar los demás records del batch

        except Exception as e:
            logger.error(f"Error procesando record: {e}", exc_info=True)
            results.append({'file': filename, 'status': 'ERROR', 'error': str(e)})
            continue

    logger.info("=== Router Complete ===")
    logger.info(f"Results: {json.dumps(results)}")

    return {
        'statusCode': 200,
        'body': json.dumps({'results': results})
    }