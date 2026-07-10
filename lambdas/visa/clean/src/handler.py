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
    logger.info(f"Downloading into buffer: s3://{STAGING_BUCKET}/{s3_key}")
    response = s3.get_object(Bucket=STAGING_BUCKET, Key=s3_key)
    return BytesIO(response['Body'].read())

# =============================================================================
# LÓGICA DE LIMPIEZA
# =============================================================================

def _parse_dates(date_series: pd.Series, date_format: str, file_date: str) -> pd.Series:
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
    return field_series.str.strip().replace('', ' ')

def _clean_integer(field_series: pd.Series) -> pd.Series:
    return pd.to_numeric(field_series.fillna('0').astype(str).str.strip(), errors='coerce').fillna(0).astype('Int64')

def _clean_float(field_series: pd.Series, float_decimals: int) -> pd.Series:
    pre = field_series.fillna('0').astype(str)
    for char, digit in EBCDIC_OVERPUNCH_ALL.items():
        pre = pre.str.replace(char, digit, regex=False)
    return pd.to_numeric(pre.str.strip(), errors='coerce').fillna(0) / (10 ** float_decimals)

def _clean_date(field_series: pd.Series, date_format: str, file_date: str) -> pd.Series:
    return _parse_dates(field_series.astype(str).str.strip(), date_format, file_date)

def _clean_field_values(field_series: pd.Series, field_def: Dict[str, Any], file_date: str) -> pd.Series:
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
