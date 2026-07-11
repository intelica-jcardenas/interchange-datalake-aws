"""
handler.py — Lambda real: itl-0004-itx-dev-intchg-02-lmbd-vi-clean
================================================================================
Archivo:     lambdas/visa/clean/src/handler.py

Cuarta etapa del pipeline Visa (tras transform y extract). Normaliza y da
formato final a los campos extraídos de cada tipo de record (BASEII, SMS,
VSS_110/120/130/140), usando las definiciones de campo de la tabla DynamoDB
`visa_fields` (columna `type_record` + GSI `type-record-index`). Castea cada
columna a su tipo declarado (string/int/float/date), aplica las estrategias
de parseo de fecha propias de Visa (`!YDDD`, `!YDDD_MAX`, `!MMDD`,
`!YYYYDDD`) y limpia máscaras no numéricas de `account_number`. Procesa el
Parquet de extract en streaming (`iter_batches` + `ParquetWriter`) para no
cargar el archivo completo en memoria, y escribe el resultado a
`s3-staging/300_*_cln_*/`. Este es el Parquet con los campos originales en
su forma final correcta — la siguiente etapa (`glue-vi-calculate`) ya no
toca estos valores, solo agrega columnas derivadas.

Flujo:
1. Por cada output de extract (uno por type_record): cargar definiciones de
   campo desde DynamoDB
2. Descargar el Parquet de extract a un buffer en memoria
3. Iterar el Parquet en chunks (CLEAN_CHUNK_SIZE, default 400000 filas)
4. Por chunk: limpiar cada columna según su tipo declarado
5. Forzar timestamps a microsegundos (compatibilidad con Spark downstream)
6. Escribir cada chunk al ParquetWriter en streaming
7. Subir el Parquet consolidado a s3-staging

Variables de entorno:
  S3_BUCKET_STAGING          : bucket de staging (lectura de extract, escritura de clean)
  DYNAMODB_FIELD_DEFINITION  : tabla de definición de campos Visa (default: itx-visa-fields)
  CLEAN_CHUNK_SIZE           : filas por chunk de streaming (default: 400000)
"""
import os
import json
import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from boto3.dynamodb.conditions import Key
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

STAGING_BUCKET = os.environ.get('S3_BUCKET_STAGING')
FIELD_DEF_TABLE = os.environ.get('DYNAMODB_FIELD_DEFINITION', 'itx-visa-fields')

OUTPUT_TYPE_CONFIG = {
    "BASEII": {"type_record": "draft", "input_subdir": "200_baseii_ext_drafts", "output_subdir": "300_baseii_cln_drafts"},
    "SMS": {"type_record": "sms", "input_subdir": "200_sms_ext_messages", "output_subdir": "300_sms_cln_messages"},
    "VSS_110": {"type_record": "vss_110", "input_subdir": "200_vss_110_ext", "output_subdir": "300_vss_110_cln"},
    "VSS_120": {"type_record": "vss_120", "input_subdir": "200_vss_120_ext", "output_subdir": "300_vss_120_cln"},
    "VSS_130": {"type_record": "vss_130", "input_subdir": "200_vss_130_ext", "output_subdir": "300_vss_130_cln"},
    "VSS_140": {"type_record": "vss_140", "input_subdir": "200_vss_140_ext", "output_subdir": "300_vss_140_cln"},
}

EBCDIC_OVERPUNCH_ALL = {
    '{': '0', 'A': '1', 'B': '2', 'C': '3', 'D': '4', 'E': '5', 'F': '6', 'G': '7', 'H': '8', 'I': '9',
    '}': '0', 'J': '1', 'K': '2', 'L': '3', 'M': '4', 'N': '5', 'O': '6', 'P': '7', 'Q': '8', 'R': '9',
}

# =============================================================================
# FUNCIONES DE ACCESO A DATOS
# =============================================================================

def _load_field_definitions(type_record: str) -> pd.DataFrame:
    """
    Consulta en DynamoDB (tabla `visa_fields`, GSI `type-record-index`) todas
    las definiciones de campo para un `type_record` dado (ej. "draft", "sms",
    "vss_110"), paginando con `LastEvaluatedKey` hasta agotar los resultados.

    Args:
        type_record: Tipo de record Visa a consultar, ej. "draft" para BASEII.

    Returns:
        DataFrame con una fila por campo definido (columnas `column_name`,
        `column_type`, `date_format`, `float_decimals`, etc.), o un
        DataFrame vacío si no hay definiciones para ese `type_record`.

    Ejemplo:
        _load_field_definitions("draft")  # DataFrame con ~90 campos BASEII
    """
    table = dynamodb.Table(FIELD_DEF_TABLE)
    response = table.query(IndexName='type-record-index', KeyConditionExpression=Key('type_record').eq(type_record))
    items = response.get('Items', [])
    while 'LastEvaluatedKey' in response:
        response = table.query(IndexName='type-record-index', KeyConditionExpression=Key('type_record').eq(type_record), ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))
    if not items:
        return pd.DataFrame()
    df = pd.DataFrame(items)
    if 'float_decimals' in df.columns:
        df['float_decimals'] = pd.to_numeric(df['float_decimals'], errors='coerce').fillna(0).astype(int)
    return df

def _get_s3_file_object(s3_key: str) -> BytesIO:
    """
    Descarga un objeto de `S3_BUCKET_STAGING` completo a un buffer en memoria.

    Args:
        s3_key: Key del objeto dentro del bucket de staging.

    Returns:
        Buffer `BytesIO` con el contenido del objeto.

    Ejemplo:
        _get_s3_file_object("EBGR/VISA/200_baseii_ext_drafts/.../x.parquet")
    """
    logger.info(f"Downloading into buffer: s3://{STAGING_BUCKET}/{s3_key}")
    response = s3.get_object(Bucket=STAGING_BUCKET, Key=s3_key)
    return BytesIO(response['Body'].read())

# =============================================================================
# LÓGICA DE LIMPIEZA
# =============================================================================

def _parse_dates(date_series: pd.Series, date_format: str, file_date: str) -> pd.Series:
    """
    Convierte una Serie de strings crudos a fechas, según el `date_format`
    declarado en DynamoDB para el campo. Además de los formatos estándar de
    `datetime.strptime` (cualquier `date_format` que empiece con "%"),
    soporta las convenciones propias de Visa `!MMDD`, `!YDDD`, `!YDDD_MAX`
    y `!YYYYDDD` — cada una con su propia estrategia de reconstrucción de
    año, documentada en el comentario inline de su rama correspondiente
    más abajo en el código.

    Args:
        date_series: Serie de valores de fecha crudos (string).
        date_format: Formato declarado en DynamoDB para el campo, ej.
            "!YDDD".
        file_date: Fecha del archivo en formato "YYYY-MM-DD", usada como
            referencia para reconstruir el año y como fallback para
            valores inválidos ("0000").

    Returns:
        Serie de `Timestamp` (o `NaT` para valores no parseables). Lanza
        `NotImplementedError` si `date_format` no es ninguno de los
        formatos soportados.

    Ejemplo:
        _parse_dates(pd.Series(['6004']), '!YDDD', '2026-01-03')  # 2026-01-04
    """
    reference_date = datetime.strptime(file_date, "%Y-%m-%d")
    reference_date_ts = pd.Timestamp(reference_date)
    if date_format.startswith('%'):
        return pd.to_datetime(date_series, format=date_format, errors='coerce')
    if date_format == '!MMDD':
        # Campos con formato Visa MMDD (mes y dia, sin anio): purchase_date.
        #
        # El anio se infiere comparando el MES del campo contra el MES de file_date:
        #   MM del campo <= MM de file_date  ->  mismo anio que file_date
        #   MM del campo >  MM de file_date  ->  anio anterior
        #
        # Esto respeta la regla Visa: purchase_date debe estar dentro de los 11 meses
        # anteriores al central_processing_date. Comparar solo el mes (no la fecha completa)
        # evita el error de restar un anio cuando el dia es 1-2 dias posterior a file_date
        # dentro del mismo mes -- lo cual es valido porque el central_processing_date de
        # cada transaccion puede ser ligeramente mayor al file_date del archivo.
        #
        # '0000' -> file_date: fecha no disponible segun spec Visa, se usa el file_date como
        # proxy para evitar NaT en calculos posteriores (ej. timeliness).
        s = date_series.astype(str).str.strip()
        src_month = pd.to_numeric(s.str[:2], errors='coerce')
        year = pd.Series(
            reference_date.year - (src_month > reference_date.month).astype(int),
            index=s.index,
        )
        pre = pd.to_datetime(year.astype(str) + s, format="%Y%m%d", errors='coerce')
        pre[s == '0000'] = reference_date_ts
        return pre
    if date_format == '!YDDD':
        # Campos con formato Visa YDDD (ultimo digito del anio + dia juliano):
        # central_processing_date y account_reference_number_date.
        #
        # El anio completo se reconstruye como: decada_de(file_date) + digito Y del campo.
        # Ejemplo: file_date=2026-01-03, campo='6004' -> '2' + '6004' -> parse '%y%j'
        #          -> anio=26 -> 2026, dia=4 -> 2026-01-04.
        #
        # No se aplica ninguna correccion posterior aunque el resultado sea mayor a file_date,
        # porque estos campos pueden ser legitimamente 1-2 dias posteriores al file_date
        # (el VIC procesa transacciones en dias consecutivos dentro del mismo archivo).
        #
        # '0000' -> file_date: valor invalido YDDD, se usa la fecha del archivo como proxy.
        s = date_series.astype(str).str.strip()
        pre = pd.to_datetime(str(reference_date.year)[2] + s, format="%y%j", errors='coerce')
        pre[s == '0000'] = reference_date_ts
        return pre
    if date_format == '!YDDD_MAX':
        # Identico a !YDDD con una unica consideracion adicional: la fecha resultante
        # no puede ser posterior a file_date (aplica a conversion_date, que contiene la
        # fecha del archivo de tasas usado -- un archivo de tasas del futuro es imposible).
        #
        # Si el resultado supera file_date se resta 1 anio para obtener la fecha correcta.
        # Ejemplo: file_date=2026-01-03 (dia 3), campo='6004' (dia 4) -> decodifica 2026-01-04
        #          -> 2026-01-04 > 2026-01-03 -> restar 1 anio -> 2025-01-04.
        #
        # '0000' -> file_date: consistente con el resto de campos YDDD.
        s = date_series.astype(str).str.strip()
        pre = pd.to_datetime(str(reference_date.year)[2] + s, format="%y%j", errors='coerce')
        future_mask = pre > reference_date_ts
        pre.loc[future_mask] = pre.loc[future_mask] - pd.DateOffset(years=1)
        pre[s == '0000'] = reference_date_ts
        return pre
    if date_format == '!YYYYDDD':
        def parse_yyyy_ddd(ds):
            try:
                return datetime(int(str(ds)[:4]), 1, 1) + timedelta(days=int(str(ds)[4:]) - 1)
            except:
                return pd.NaT
        return pd.to_datetime(date_series.apply(parse_yyyy_ddd))
    raise NotImplementedError(f"Format not supported: {date_format}")

def _clean_string(field_series: pd.Series) -> pd.Series:
    """
    Recorta espacios en blanco de una Serie de strings; los valores que
    quedan vacíos tras el strip se reemplazan por un único espacio (nunca
    string vacío), para no perder la columna como NaN en pasos posteriores.

    Args:
        field_series: Serie de valores string a limpiar.

    Returns:
        Serie con strings recortados, sin valores vacíos.

    Ejemplo:
        _clean_string(pd.Series(['  ABC ', '   ']))  # ['ABC', ' ']
    """
    return field_series.str.strip().replace('', ' ')

def _clean_integer(field_series: pd.Series) -> pd.Series:
    """
    Convierte una Serie a enteros nullable (`Int64`), tratando valores
    nulos o no numéricos como 0.

    Args:
        field_series: Serie de valores a convertir.

    Returns:
        Serie de tipo `Int64` (entero nullable de pandas).

    Ejemplo:
        _clean_integer(pd.Series(['007', None]))  # [7, 0]
    """
    return pd.to_numeric(field_series.fillna('0').astype(str).str.strip(), errors='coerce').fillna(0).astype('Int64')

def _clean_float(field_series: pd.Series, float_decimals: int) -> pd.Series:
    """
    Convierte una Serie a float aplicando el divisor implícito de Visa
    (`float_decimals`, cantidad de decimales implícitos en el campo de
    ancho fijo) y traduciendo overpunch EBCDIC (signo codificado en el
    último dígito) a su dígito numérico equivalente antes de parsear.

    Args:
        field_series: Serie de valores crudos (string de ancho fijo).
        float_decimals: Cantidad de decimales implícitos a dividir, según
            la definición de campo en DynamoDB.

    Returns:
        Serie de tipo float, con nulos/no numéricos tratados como 0.

    Ejemplo:
        _clean_float(pd.Series(['00123']), 2)  # [1.23]
    """
    pre = field_series.fillna('0').astype(str)
    for char, digit in EBCDIC_OVERPUNCH_ALL.items():
        pre = pre.str.replace(char, digit, regex=False)
    return pd.to_numeric(pre.str.strip(), errors='coerce').fillna(0) / (10 ** float_decimals)

def _clean_date(field_series: pd.Series, date_format: str, file_date: str) -> pd.Series:
    """
    Wrapper de `_parse_dates()` que primero normaliza la Serie a string
    recortado antes de delegar el parseo.

    Args:
        field_series: Serie de valores de fecha crudos.
        date_format: Formato declarado en DynamoDB para el campo.
        file_date: Fecha del archivo en formato "YYYY-MM-DD".

    Returns:
        Serie de `Timestamp` resultante de `_parse_dates()`.

    Ejemplo:
        _clean_date(pd.Series(['6004']), '!YDDD', '2026-01-03')  # 2026-01-04
    """
    return _parse_dates(field_series.astype(str).str.strip(), date_format, file_date)

def _clean_field_values(field_series: pd.Series, field_def: Dict[str, Any], file_date: str) -> pd.Series:
    """
    Despacha la limpieza de una columna según su `column_type` declarado en
    DynamoDB (`str`, `int`, `float`, `date`), aplicando la función de
    limpieza correspondiente. Cualquier tipo no reconocido cae al
    tratamiento de string.

    Args:
        field_series: Serie de valores crudos de la columna.
        field_def: Definición del campo desde DynamoDB (`column_type`,
            `float_decimals`, `date_format`, etc.).
        file_date: Fecha del archivo en formato "YYYY-MM-DD", usada por la
            limpieza de fechas.

    Returns:
        Serie limpia con el tipo correspondiente a `column_type`.

    Ejemplo:
        _clean_field_values(pd.Series(['00123']), {'column_type': 'int'}, '2026-01-03')  # [123]
    """
    col_type = field_def.get('column_type', 'str')
    if col_type == 'str':
        return _clean_string(field_series)
    elif col_type == 'int':
        return _clean_integer(field_series)
    elif col_type == 'float':
        return _clean_float(field_series, int(field_def.get('float_decimals', 2)))
    elif col_type == 'date':
        return _clean_date(field_series, field_def.get('date_format'), file_date)
    return _clean_string(field_series)

def _clean_chunk(chunk_df: pd.DataFrame, field_defs_dict: dict, file_date: str):
    """
    Limpia todas las columnas de un chunk del Parquet de extract, una por
    una, usando la definición de campo correspondiente. La columna
    `record` (identificador de fila, no un campo de negocio) se preserva
    sin transformar. `account_number` recibe además una limpieza
    específica: cualquier carácter no numérico (máscaras `*`/`?` de PAN
    parcial) se reemplaza por `'0'`, replicando el comportamiento del
    legacy. Las columnas sin definición en DynamoDB, o cuya limpieza
    falla, quedan como string sin normalizar — nunca se descarta una
    columna del chunk.

    Args:
        chunk_df: DataFrame con el chunk crudo leído de extract.
        field_defs_dict: Definiciones de campo indexadas por `column_name`.
        file_date: Fecha del archivo en formato "YYYY-MM-DD".

    Returns:
        Tupla `(DataFrame limpio, cantidad de columnas limpiadas
        exitosamente)`.

    Ejemplo:
        _clean_chunk(chunk_df, field_defs_dict, '2026-01-03')  # (df_limpio, 42)
    """
    cleaned_fields = []
    fields_cleaned = 0

    for col in chunk_df.columns:
        if col == 'record':
            cleaned_fields.append(chunk_df[col])
            continue
            
        f_def = field_defs_dict.get(col)
        if f_def:
            try:
                cleaned = _clean_field_values(chunk_df[col], f_def, file_date)
                if col == 'account_number':
                    cleaned = cleaned.str.replace(r'\D', '0', regex=True)
                cleaned.name = col
                cleaned_fields.append(cleaned)
                fields_cleaned += 1
            except:
                cleaned_fields.append(chunk_df[col].astype(str))
        else:
            cleaned = chunk_df[col].astype(str).str.strip()
            cleaned.name = col
            cleaned_fields.append(cleaned)

    return pd.concat(cleaned_fields, axis=1), fields_cleaned

# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def clean_output(output: Dict, file_date: str, client_id: str, brand: str,
                 file_type: str, content_hash: str) -> Optional[Dict]:
    """
    Limpia un output de extract (un `type_record` de un archivo, ej. BASEII
    o VSS_120) de punta a punta: carga las definiciones de campo, descarga
    el Parquet de extract, lo procesa en streaming por chunks de
    `CLEAN_CHUNK_SIZE` filas (`iter_batches` + `ParquetWriter`, para no
    cargar el archivo completo en memoria), fuerza los timestamps a
    microsegundos (compatibilidad con `spark.read.parquet()` downstream) y
    sube el Parquet consolidado a `s3-staging/300_*_cln_*/`.

    Args:
        output: Dict de un output de extract, con `output_type`, `s3_key`
            y `records`.
        file_date: Fecha del archivo en formato "YYYY-MM-DD".
        client_id: Código del cliente (no usado para lógica, solo
            trazabilidad).
        brand: Marca del archivo ("VISA"), no usado para lógica, solo
            trazabilidad.
        file_type: "IN" u "OUT", no usado para lógica, solo trazabilidad.
        content_hash: MD5 del archivo origen, no usado para lógica en esta
            función (se propaga vía el `record` heredado del extract).

    Returns:
        Dict con el resultado de la limpieza (`output_type`, `s3_key`,
        `records`, `fields_cleaned`, `total_columns`), o `None` si el
        `output_type` no está en `OUTPUT_TYPE_CONFIG` o no hay
        definiciones de campo para su `type_record`. Relanza cualquier
        excepción tras loguearla.

    Ejemplo:
        clean_output({'output_type': 'BASEII', 's3_key': '...', 'records': 1000},
                      '2026-01-03', 'EBGR', 'VISA', 'IN', 'AB12...')
    """
    out_type = output.get('output_type')
    in_key = output.get('s3_key')
    in_records = output.get('records', 0)

    config = OUTPUT_TYPE_CONFIG.get(out_type)
    if not config:
        return None

    try:
        field_defs = _load_field_definitions(config['type_record'])
        if field_defs.empty:
            return None
        field_defs_dict = {
            row['column_name']: row.to_dict()
            for _, row in field_defs.iterrows()
            if 'column_name' in row
        }

        file_obj = _get_s3_file_object(in_key)
        parquet_file = pq.ParquetFile(file_obj)

        out_key = in_key.replace(config['input_subdir'], config['output_subdir'])
        output_buffer = BytesIO()
        writer = None

        records_written = 0
        total_fields_cleaned = 0
        chunk_size = int(os.environ.get('CLEAN_CHUNK_SIZE', '400000'))

        logger.info(f"Starting chunked clean for {out_type}. Chunk size: {chunk_size}")

        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            chunk_df = batch.to_pandas()

            if chunk_df.empty:
                continue

            cleaned_chunk, fields_cleaned = _clean_chunk(chunk_df, field_defs_dict, file_date)

            cleaned_table = pa.Table.from_pandas(
                cleaned_chunk,
                preserve_index=False
            )

            # 👇 FIX: BLINDAJE DE INTEROPERABILIDAD PARA SPARK 👇
            # Forzamos todas las fechas de Nanosegundos (ns) a Microsegundos (us)
            new_fields = []
            for field in cleaned_table.schema:
                if pa.types.is_timestamp(field.type):
                    new_fields.append(pa.field(field.name, pa.timestamp('us', tz=field.type.tz)))
                else:
                    new_fields.append(field)
            
            # Aplicar el esquema corregido
            cleaned_table = cleaned_table.cast(pa.schema(new_fields), safe=False)
            # 👆 FIN DEL FIX 👆

            if writer is None:
                writer = pq.ParquetWriter(
                    output_buffer,
                    cleaned_table.schema,
                    compression='snappy',
                    coerce_timestamps='us',
                    allow_truncated_timestamps=True
                )
                total_fields_cleaned = fields_cleaned

            writer.write_table(cleaned_table)
            records_written += len(cleaned_chunk)
            logger.info(f"  Cleaned batch. Total records so far: {records_written}")

        if writer is not None:
            writer.close()
            output_buffer.seek(0)
            s3.put_object(
                Bucket=STAGING_BUCKET,
                Key=out_key,
                Body=output_buffer.getvalue()
            )
        else:
            logger.warning(f"No valid records processed for {out_type}")
            return None

        file_obj.close()
        output_buffer.close()

        return {
            'output_type': out_type,
            'type_record': config['type_record'],
            'input_subdir': config['input_subdir'],
            'output_subdir': config['output_subdir'],
            'input_s3_key': in_key,
            's3_key': out_key,
            'input_records': in_records,
            'records': records_written,
            'fields_cleaned': total_fields_cleaned,
            'total_columns': len(cleaned_table.schema.names) if writer else 0
        }

    except Exception as e:
        logger.error(f"Error cleaning {out_type}: {str(e)}", exc_info=True)
        raise

# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Punto de entrada de la Lambda `lmbd-vi-clean`. Invocada por la Step
    Function Visa tras `lmbd-vi-extract`. Recorre todos los outputs de
    extract recibidos en el evento, limpia cada uno con `clean_output()`
    de forma independiente (un fallo en un output no aborta los demás) y
    agrega el resultado en un único payload de salida para la siguiente
    etapa (`glue-vi-calculate`).

    Args:
        event: Payload de Step Functions con `client_id`, `file_id`,
            `brand`, `file_type`, `file_date`, `content_hash` y `outputs`
            (lista de outputs de extract a limpiar).
        context: Contexto de ejecución de Lambda (no usado).

    Returns:
        Dict con `status` ("SUCCESS", "PARTIAL_SUCCESS" o "ERROR"),
        `total_outputs`, `total_records`, `total_fields_cleaned`, la lista
        `outputs` con el resultado de cada limpieza exitosa, y `errors`
        con el detalle de los outputs que fallaron.

    Ejemplo:
        lambda_handler({'client_id': 'EBGR', 'file_id': '...', 'brand': 'VISA',
                         'file_type': 'IN', 'file_date': '2026-01-03',
                         'content_hash': '...', 'outputs': [...]}, None)
    """
    logger.info("=" * 70)
    logger.info("ITX CLEAN LAMBDA - START (WITH CHUNKING)")
    logger.info("=" * 70)

    client_id = event.get('client_id')
    file_id = event.get('file_id')
    brand = event.get('brand')
    file_type = event.get('file_type')
    file_date = event.get('file_date')
    content_hash = event.get('content_hash')

    if not file_date:
        raise ValueError("file_date is required in event")

    extract_outputs = event.get('outputs', [])
    clean_outputs, errors = [], []

    for output in extract_outputs:
        try:
            result = clean_output(output, file_date, client_id, brand, file_type, content_hash)
            if result:
                clean_outputs.append(result)
        except Exception as e:
            errors.append({
                'output_type': output.get('output_type'),
                'input_s3_key': output.get('s3_key'),
                'error': str(e)
            })

    status = 'ERROR' if errors and not clean_outputs else ('PARTIAL_SUCCESS' if errors else 'SUCCESS')

    return {
        'status': status,
        'total_outputs': len(clean_outputs),
        'total_records': sum(o.get('records', 0) for o in clean_outputs),
        'total_fields_cleaned': sum(o.get('fields_cleaned', 0) for o in clean_outputs),
        'outputs': clean_outputs,
        'errors': errors if errors else None,
        'client_id': client_id,
        'file_id': file_id,
        'brand': brand,
        'file_type': file_type,
        'file_date': file_date,
        'content_hash': content_hash
    }
