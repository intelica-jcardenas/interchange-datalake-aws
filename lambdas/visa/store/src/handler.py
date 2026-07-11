"""
handler.py — Lambda real: itl-0004-itx-dev-intchg-02-lmbd-vi-store
================================================================================
Archivo:     lambdas/visa/store/src/handler.py

Última etapa del pipeline Visa antes del archive. Une los tres Parquets
clave de staging (CLN, CAL, ITX) en un solo Parquet consolidado por
output_type y lo escribe al bucket operational — sin este paso no existe
el archivo que Athena consultará vía crawler. El join es por índice
`record` (no por llave de negocio como en Mastercard, ver
`decisions.md` → "Por qué mc-store fusiona CLN + CAL + ITX por llave"): CAL
e ITX se cargan completos en memoria (pocas columnas, ~50), mientras que
CLN se procesa en streaming por chunks (muchas columnas, ~250) para
acotar el pico de memoria. Restaura los tipos Arrow originales del CAL
(`_cal_dtype_map`) tras el round-trip por pandas, que degrada enteros con
nulls a `float64` y columnas 100% null a `NullType` — ver `decisions.md`
→ "Por qué lmbd-vi-store lee el CAL con _read_parquet_arrow".

Mapeo de subdirectorios por output_type:
  BASEII:
    300_baseii_cln_drafts  → transactions (clean)
    400_baseii_cal_drafts  → calculated   (Glue calculate)
    500_baseii_itx_drafts  → interchange  (Glue interchange)
    → operational: baseii_drafts

  SMS:
    300_sms_cln_messages   → transactions (clean)
    400_sms_cal_messages   → calculated   (Glue calculate)
    500_sms_itx_messages   → interchange  (Glue interchange)
    → operational: sms_messages

  VSS (110/120/130/140) — sin interchange:
    300_baseii_cln_vss_{type} → transactions (clean)
    400_baseii_cal_vss_{type} → calculated   (Glue calculate)
    → operational: baseii_vss_{type}

Variables de entorno:
  S3_BUCKET_STAGING           : bucket de staging (origen de CLN/CAL/ITX)
  S3_BUCKET_OPERATIONAL       : bucket operational (destino del Parquet final)
  DYNAMODB_TABLE_FILE_CONTROL : tabla de control de archivos
  STORE_CHUNK_SIZE            : filas por chunk al procesar CLN (default: 80000)
"""

import os
import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from typing import Optional, Dict, List

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3       = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

STAGING_BUCKET     = os.environ.get('S3_BUCKET_STAGING')
OPERATIONAL_BUCKET = os.environ.get('S3_BUCKET_OPERATIONAL')
FILE_CONTROL_TABLE = os.environ.get('DYNAMODB_TABLE_FILE_CONTROL', 'itx-file-control')

# =============================================================================
# MAPEO DE SUBDIRECTORIOS POR OUTPUT TYPE
# Equivalente a los parámetros de store_baseii_file, store_sms_file,
# store_vss_file en el código local.
# =============================================================================

OUTPUT_TYPE_CONFIG = {
    "BASEII": {
        "cln_subdir": "300_baseii_cln_drafts",
        "cal_subdir": "400_baseii_cal_drafts",
        "itx_subdir": "500_baseii_itx_drafts",
        "target_subdir": "baseii_drafts",
        "has_interchange": True,
    },
    "SMS": {
        "cln_subdir": "300_sms_cln_messages",
        "cal_subdir": "400_sms_cal_messages",
        "itx_subdir": "500_sms_itx_messages",
        "target_subdir": "sms_messages",
        "has_interchange": True,
    },
    "VSS_110": {
        "cln_subdir": "300_vss_110_cln",
        "cal_subdir": "400_vss_110_cal",
        "itx_subdir": None,
        "target_subdir": "vss_110",
        "has_interchange": False,
    },
    "VSS_120": {
        "cln_subdir": "300_vss_120_cln",
        "cal_subdir": "400_vss_120_cal",
        "itx_subdir": None,
        "target_subdir": "vss_120",
        "has_interchange": False,
    },
    "VSS_130": {
        "cln_subdir": "300_vss_130_cln",
        "cal_subdir": "400_vss_130_cal",
        "itx_subdir": None,
        "target_subdir": "vss_130",
        "has_interchange": False,
    },
    "VSS_140": {
        "cln_subdir": "300_vss_140_cln",
        "cal_subdir": "400_vss_140_cal",
        "itx_subdir": None,
        "target_subdir": "vss_140",
        "has_interchange": False,
    },
}

# =============================================================================
# HELPERS
# =============================================================================

def _read_parquet_from_s3(bucket: str, s3_key: str) -> pd.DataFrame:
    """
    Lee un Parquet de S3 como DataFrame de pandas, soportando tanto un
    archivo único (típico de un output de Lambda, ej. ITX) como un
    directorio con part-files de Spark (típico de un output de Glue). Para
    el caso directorio, concatena todos los part-files ordenados por
    nombre.

    Args:
        bucket: Bucket S3 donde está el Parquet.
        s3_key: Key del archivo único, o prefix del directorio con
            part-files.

    Returns:
        DataFrame con el contenido completo (archivo único o concatenación
        de todos los part-files).

    Raises:
        FileNotFoundError: si `s3_key` no es un archivo único y el
            directorio no tiene part-files.

    Ejemplo:
        _read_parquet_from_s3(bucket, "SBSA/VISA/500_baseii_itx_drafts/.../")
    """
    logger.info(f"Reading s3://{bucket}/{s3_key}")

    # Intentar como archivo único primero (CLN — output de Lambda)
    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        table = pq.read_table(BytesIO(response['Body'].read()))
        logger.info(f"  Read as single file: {len(table):,} rows")
        return table.to_pandas()
    except Exception as e:
        if 'NoSuchKey' not in str(e) and '404' not in str(e):
            raise  # error distinto a "no existe" — relanzar

    # Fallback: es un directorio PySpark con part files
    prefix = s3_key if s3_key.endswith('/') else s3_key + '/'
    logger.info(f"  Not a single file — scanning part files in {prefix}")

    paginator = s3.get_paginator('list_objects_v2')
    part_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.parquet') and 'part-' in key:
                part_keys.append(key)

    if not part_keys:
        raise FileNotFoundError(
            f"No part files found in s3://{bucket}/{prefix}"
        )

    logger.info(f"  Found {len(part_keys)} part files — reading...")
    tables = []
    for key in sorted(part_keys):
        response = s3.get_object(Bucket=bucket, Key=key)
        tables.append(pq.read_table(BytesIO(response['Body'].read())))
        logger.info(f"    {key.split('/')[-1]}: {len(tables[-1]):,} rows")

    merged = pa.concat_tables(tables)
    logger.info(f"  Total after concat: {len(merged):,} rows")
    return merged.to_pandas()


def _write_parquet_to_s3(df: pd.DataFrame, bucket: str, s3_key: str) -> int:
    """
    Escribe un DataFrame completo como un único Parquet en S3, vía un
    buffer en memoria (sin pasar por disco).

    Args:
        df: DataFrame a escribir.
        bucket: Bucket S3 destino.
        s3_key: Key del archivo Parquet a escribir.

    Returns:
        Cantidad de records escritos (`len(df)`). Relanza cualquier
        excepción tras loguearla.

    Ejemplo:
        _write_parquet_to_s3(df, bucket, "SBSA/VISA/baseii_drafts/.../x.parquet")
    """
    logger.info(f"Writing {len(df):,} records to s3://{bucket}/{s3_key}")
    try:
        table  = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())
        logger.info(f"Saved {len(df):,} records → s3://{bucket}/{s3_key}")
        return len(df)
    except Exception as e:
        logger.error(f"Error writing s3://{bucket}/{s3_key}: {str(e)}")
        raise

def _read_parquet_arrow(bucket: str, key: str) -> pa.Table:
    """
    Igual que `_read_parquet_from_s3()` (soporta archivo único o
    directorio con part-files), pero devuelve una `pa.Table` de PyArrow en
    vez de convertir a pandas. Se usa para el CAL específicamente, porque
    preserva el schema Arrow original (necesario para `_cal_dtype_map` en
    `store_output`, que restaura tipos degradados por el round-trip a
    pandas más adelante).

    Args:
        bucket: Bucket S3 donde está el Parquet.
        key: Key del archivo único, o prefix del directorio con
            part-files.

    Returns:
        `pa.Table` con el contenido completo (archivo único o
        concatenación de todos los part-files).

    Ejemplo:
        _read_parquet_arrow(bucket, "SBSA/VISA/400_baseii_cal_drafts/.../")
    """
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return pq.read_table(BytesIO(response['Body'].read()))
    except s3.exceptions.NoSuchKey:
        pass

    prefix = key if key.endswith('/') else key + '/'

    paginator = s3.get_paginator('list_objects_v2')
    tables = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            k = obj['Key']
            if k.endswith('.parquet') and 'part-' in k:
                r = s3.get_object(Bucket=bucket, Key=k)
                tables.append(pq.read_table(BytesIO(r['Body'].read())))

    return pa.concat_tables(tables, mode="default")



def _build_s3_key(
    clean_s3_key: str,
    cln_subdir: str,
    target_subdir: str,
    bucket_type: str = "operational"
) -> str:
    """
    Construye el S3 key de destino reemplazando el subdir de CLN por otro
    subdir (CAL, ITX, o el subdir final de operational) — el resto del
    path (cliente/marca/file_type/date/hash) es idéntico entre las 3
    etapas de staging y el archivo final.

    Nota: pese al parámetro `bucket_type`, la función no cambia el bucket
    ni el prefix — solo hace el `.replace()` de subdirectorio. El caller
    (`store_output`) es responsable de escribir en el bucket correcto
    (`OPERATIONAL_BUCKET` vs `STAGING_BUCKET`).

    Args:
        clean_s3_key: S3 key del Parquet CLN (staging), fuente de verdad
            del resto del path.
        cln_subdir: Subdir de CLN a reemplazar, ej. "300_baseii_cln_drafts".
        target_subdir: Subdir de destino, ej. "400_baseii_cal_drafts" o
            "baseii_drafts" (operational).
        bucket_type: No usado actualmente en la lógica — ver nota arriba.

    Returns:
        El `clean_s3_key` con el subdir reemplazado.

    Ejemplo:
        _build_s3_key(
            "SBSA/VISA/300_baseii_cln_drafts/file_type=OUT/date=2026-04-04/HASH.parquet",
            "300_baseii_cln_drafts", "baseii_drafts"
        )  # "SBSA/VISA/baseii_drafts/file_type=OUT/date=2026-04-04/HASH.parquet"
    """
    return clean_s3_key.replace(cln_subdir, target_subdir)

# =============================================================================
# FUNCIÓN PRINCIPAL DE STORE POR OUTPUT TYPE
# =============================================================================

def store_output(
    output: Dict,
    client_id: str,
    brand: str,
    file_type: str,
    file_date: str,
    content_hash: str,
) -> Optional[Dict]:
    """
    Realiza el merge de los 3 Parquets (CLN + CAL + ITX) para un
    output_type y guarda el resultado consolidado en operational.

    Estrategia de memoria:
      - CAL e ITX se cargan completos en RAM (pocas columnas, ~50)
      - CLN se procesa en chunks de STORE_CHUNK_SIZE filas (muchas
        columnas, ~250) — nunca se carga completo
      - Join por índice 'record' (left join CLN+CAL, luego +ITX si aplica)
        — el resultado es idéntico al de un join completo porque cada
        chunk de CLN solo necesita las filas de CAL/ITX cuyo índice
        interseca ese chunk

    Tras el merge, restaura en `merged_table` los tipos Arrow originales
    del CAL (`_cal_dtype_map`, capturado antes del round-trip a pandas) —
    ver comentario inline antes del loop de restauración para el detalle
    de qué degrada el round-trip y por qué. A partir del segundo batch,
    fuerza el schema del primer batch (`merged_table.cast(schema)`) para
    evitar `Table schema does not match` entre chunks del mismo Parquet.

    Args:
        output: Dict de un output de clean, con `output_type` y `s3_key`
            (key del Parquet CLN).
        client_id: Código del cliente (no usado para lógica, solo
            trazabilidad).
        brand: Marca del archivo ("VISA"), no usado para lógica, solo
            trazabilidad.
        file_type: "IN" u "OUT", no usado para lógica, solo trazabilidad.
        file_date: Fecha del archivo (no usado para lógica, solo
            trazabilidad).
        content_hash: MD5 del archivo origen (no usado para lógica en esta
            función — ya viene propagado como columna desde CLN).

    Returns:
        Dict con el resultado del store (`output_type`, `cln_s3_key`,
        `cal_s3_key`, `itx_s3_key`, `target_s3_key`, `records`, `columns`,
        `batches`), o `None` si el `output_type` no está en
        `OUTPUT_TYPE_CONFIG` o no se escribió ningún registro. Relanza
        cualquier excepción tras loguearla.

    Ejemplo:
        store_output({'output_type': 'BASEII', 's3_key': '.../300_baseii_cln_drafts/.../x.parquet'},
                      'SBSA', 'VISA', 'OUT', '2026-04-04', '279AE7CB...')
    """
    import time

    output_type  = output.get('output_type')
    clean_s3_key = output.get('s3_key')

    logger.info(f"{'='*60}")
    logger.info(f"Processing store for: {output_type}")

    config = OUTPUT_TYPE_CONFIG.get(output_type)
    if not config:
        logger.warning(f"No config found for output_type: {output_type} — skipping")
        return None

    cln_subdir      = config['cln_subdir']
    cal_subdir      = config['cal_subdir']
    itx_subdir      = config['itx_subdir']
    target_subdir   = config['target_subdir']
    has_interchange = config['has_interchange']

    try:
        # ── Construir S3 keys ─────────────────────────────────────────────
        cal_s3_key    = clean_s3_key.replace(cln_subdir, cal_subdir)
        target_s3_key = clean_s3_key.replace(cln_subdir, target_subdir)

        # ── Paso 1: CAL completo en RAM (pocas columnas ~50) ──────────────
        # Leemos como Arrow primero para capturar los tipos originales de cada
        # columna. El round-trip por pandas degrada algunos tipos:
        #   - INT64+nulls → float64 (numpy no tiene int nullable)
        #   - string 100% null → pandas object con puros None → pyarrow no
        #     puede inferir el tipo desde los datos y le asigna NullType
        # Ambos casos se restauran más abajo a partir de _cal_dtype_map.
        t0 = time.time()
        logger.info("Loading CAL into memory...")
        _cal_arrow = _read_parquet_arrow(STAGING_BUCKET, cal_s3_key)
        _cal_dtype_map = {f.name: f.type for f in _cal_arrow.schema}
        cal_df = _cal_arrow.to_pandas()
        if 'record' in cal_df.columns:
            cal_df = cal_df.set_index('record')
        logger.info(f"  CAL: {len(cal_df):,} rows, {len(cal_df.columns)} cols [{time.time()-t0:.1f}s]")

        # ── Paso 2: ITX completo en RAM (pocas columnas, solo BASEII/SMS) ─
        itx_df = None
        if has_interchange and itx_subdir:
            itx_s3_key = clean_s3_key.replace(cln_subdir, itx_subdir)
            t1 = time.time()
            logger.info("Loading ITX into memory...")
            itx_df = _read_parquet_from_s3(STAGING_BUCKET, itx_s3_key)
            if 'record' in itx_df.columns:
                itx_df = itx_df.set_index('record')
            logger.info(f"  ITX: {len(itx_df):,} rows, {len(itx_df.columns)} cols [{time.time()-t1:.1f}s]")
        else:
            logger.info(f"ITX skipped for {output_type} (VSS — no interchange)")

        # ── Paso 3: CLN abierto para chunking (NO se carga completo) ──────
        t2 = time.time()
        logger.info("Opening CLN for chunked processing...")
        response     = s3.get_object(Bucket=STAGING_BUCKET, Key=clean_s3_key)
        file_obj     = BytesIO(response['Body'].read())
        parquet_file = pq.ParquetFile(file_obj)
        logger.info(f"  CLN opened [{time.time()-t2:.1f}s]")

        chunk_size      = int(os.environ.get('STORE_CHUNK_SIZE', '80000'))
        output_buffer   = BytesIO()
        writer          = None
        schema          = None   # se captura en el primer batch
        records_written = 0
        output_cols     = 0
        batch_num       = 0

        # ── Paso 4: Procesar CLN en chunks, join con CAL e ITX ────────────
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            chunk_df = batch.to_pandas()
            if chunk_df.empty:
                continue

            batch_num += 1
            t_batch = time.time()

            if 'record' in chunk_df.columns:
                chunk_df = chunk_df.set_index('record')

            # Join CLN chunk + CAL (por índice 'record')
            cal_chunk = cal_df.loc[cal_df.index.intersection(chunk_df.index)]
            merged    = chunk_df.join(cal_chunk, how='left', lsuffix='_cln')

            # Join con ITX (solo BASEII y SMS)
            if itx_df is not None:
                itx_chunk = itx_df.loc[itx_df.index.intersection(chunk_df.index)]
                merged    = merged.join(itx_chunk, how='left', rsuffix='_itx')

            merged = merged.reset_index()

            for col_name in merged.select_dtypes(include='object').columns:
                merged[col_name] = merged[col_name].where(pd.notna(merged[col_name]), other=None)

            # Convertir a PyArrow
            merged_table = pa.Table.from_pandas(merged)

            # Restaurar tipos de columnas del CAL que el round-trip por pandas
            # degradó: enteros con nulls → float64, y columnas 100% null →
            # NullType (pyarrow no puede inferir el tipo real desde un
            # object column con puros None). cast() desde NullType o desde
            # float64 hacia el tipo original preserva los nulls.
            for _col, _atype in _cal_dtype_map.items():
                if _col not in merged_table.schema.names:
                    continue
                _idx = merged_table.schema.get_field_index(_col)
                _current_type = merged_table.schema.field(_idx).type
                if _current_type == _atype:
                    continue
                if pa.types.is_null(_current_type) or (
                    pa.types.is_integer(_atype) and pa.types.is_floating(_current_type)
                ):
                    merged_table = merged_table.set_column(
                        _idx, _col,
                        merged_table.column(_col).cast(_atype)
                    )

            if writer is None:
                # Primer batch — capturar schema como referencia
                schema      = merged_table.schema
                writer      = pq.ParquetWriter(
                    output_buffer, schema, compression='snappy'
                )
                output_cols = len(merged.columns)
            else:
                # Batches siguientes — forzar schema del primero
                # Evita el error "Table schema does not match" entre chunks
                try:
                    merged_table = merged_table.cast(schema)
                except Exception:
                    merged_table = pa.Table.from_pandas(
                        merged, schema=schema, safe=False
                    )

            writer.write_table(merged_table)
            records_written += len(merged)
            logger.info(f"  Batch {batch_num}: +{len(merged):,} records "
                        f"(total: {records_written:,}) [{time.time()-t_batch:.1f}s]")

        if writer is None:
            logger.warning(f"No records written for {output_type}")
            file_obj.close()
            return None

        writer.close()
        output_buffer.seek(0)

        # ── Paso 5: Subir Parquet final a operational ──────────────────────
        logger.info(f"Uploading to s3://{OPERATIONAL_BUCKET}/{target_s3_key}")
        s3.put_object(
            Bucket=OPERATIONAL_BUCKET,
            Key=target_s3_key,
            Body=output_buffer.getvalue()
        )
        logger.info(f"Saved {records_written:,} records, {output_cols} cols → {target_s3_key}")

        file_obj.close()
        output_buffer.close()

        return {
            'output_type':   output_type,
            'cln_s3_key':    clean_s3_key,
            'cal_s3_key':    cal_s3_key,
            'itx_s3_key':    clean_s3_key.replace(cln_subdir, itx_subdir) if has_interchange and itx_subdir else None,
            'target_s3_key': target_s3_key,
            'records':       records_written,
            'columns':       output_cols,
            'batches':       batch_num,
        }

    except Exception as e:
        logger.error(f"Error storing {output_type}: {str(e)}", exc_info=True)
        raise

# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Punto de entrada de la Lambda `lmbd-vi-store`. Invocada por la Step
    Function Visa tras `lmbd-vi-clean` (o manualmente para reprocesos, ver
    `.claude/memory/manual_execution.md`). Recorre todos los outputs de
    clean recibidos en el evento, hace el store de cada uno con
    `store_output()` de forma independiente (un fallo en un output no
    aborta los demás) y agrega el resultado en un único payload de salida
    — última etapa antes de `lmbd-archive-file`.

    Args:
        event: Payload de Step Functions con `client_id`, `file_id`,
            `brand`, `file_type`, `file_date`, `content_hash` y `outputs`
            (lista de outputs de clean a consolidar), ej.:
            ```
            {
                "client_id":    "SBSA",
                "file_id":      "ABC123",
                "brand":        "VISA",
                "file_type":    "OUT",
                "file_date":    "2026-04-04",
                "content_hash": "279AE7CB...",
                "outputs": [
                    {
                        "output_type":   "BASEII",
                        "output_subdir": "300_baseii_cln_drafts",
                        "s3_key":        "SBSA/VISA/300_baseii_cln_drafts/file_type=OUT/date=.../HASH.parquet",
                        "records":       2369332,
                        ...
                    },
                    ...
                ]
            }
            ```
        context: Contexto de ejecución de Lambda (no usado).

    Returns:
        Dict con `status` ("SUCCESS", "PARTIAL_SUCCESS" o "ERROR"),
        `total_outputs`, `total_records`, la lista `outputs` con el
        resultado de cada store exitoso, y `errors` con el detalle de los
        outputs que fallaron. Lanza `ValueError` si falta
        `S3_BUCKET_STAGING` o `S3_BUCKET_OPERATIONAL`.

    Ejemplo:
        lambda_handler({'client_id': 'SBSA', 'file_id': 'ABC123', 'brand': 'VISA',
                         'file_type': 'OUT', 'file_date': '2026-04-04',
                         'content_hash': '279AE7CB...', 'outputs': [...]}, None)
        # {'status': 'SUCCESS', 'total_outputs': 1, 'total_records': 2369332, ...}
    """
    logger.info("=" * 70)
    logger.info("ITX STORE LAMBDA - START")
    logger.info("=" * 70)

    if not STAGING_BUCKET:
        raise ValueError("Missing environment variable: S3_BUCKET_STAGING")
    if not OPERATIONAL_BUCKET:
        raise ValueError("Missing environment variable: S3_BUCKET_OPERATIONAL")

    client_id    = event.get('client_id')
    file_id      = event.get('file_id')
    brand        = event.get('brand')
    file_type    = event.get('file_type')
    file_date    = event.get('file_date')
    content_hash = event.get('content_hash')

    clean_outputs = event.get('outputs', [])

    if not clean_outputs:
        logger.warning("No outputs received — nothing to store")
        return {'status': 'SUCCESS', 'total_outputs': 0, 'outputs': []}

    logger.info(f"Processing store for: client={client_id}, brand={brand}, "
                f"type={file_type}, date={file_date}")
    logger.info(f"Outputs to store: {[o.get('output_type') for o in clean_outputs]}")

    store_outputs = []
    errors        = []

    for output in clean_outputs:
        try:
            result = store_output(
                output=output,
                client_id=client_id,
                brand=brand,
                file_type=file_type,
                file_date=file_date,
                content_hash=content_hash,
            )
            if result:
                store_outputs.append(result)
        except Exception as e:
            errors.append({
                'output_type': output.get('output_type'),
                'error': str(e),
            })

    total_records = sum(o.get('records', 0) for o in store_outputs)

    status = ('ERROR'           if (errors and not store_outputs) else
              'PARTIAL_SUCCESS' if errors else
              'SUCCESS')

    logger.info(f"=== Done: {len(store_outputs)} outputs, "
                f"{total_records:,} records stored to operational ===")

    return {
        'status':        status,
        'total_outputs': len(store_outputs),
        'total_records': total_records,
        'outputs':       store_outputs,
        'errors':        errors if errors else None,
        'client_id':     client_id,
        'file_id':       file_id,
        'brand':         brand,
        'file_type':     file_type,
        'file_date':     file_date,
        'content_hash':  content_hash,
    }