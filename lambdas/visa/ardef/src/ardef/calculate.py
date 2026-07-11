"""
calculate.py

Cuarta etapa del pipeline ARDEF. Calcula la vigencia (valid_until) de cada
registro de rango de BIN/regla contra la tabla maestra acumulada lu_ardef
(en la capa REFERENCE), soportando llegada de registros fuera de orden, y
mantiene esa tabla maestra actualizada en S3 para que las Lambdas de
calculate de Visa (calc_ardef) puedan cruzar transacciones contra el rango
vigente en la fecha correspondiente.
"""

import gc
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pyarrow as pa

from ardef.logs.logger import Logger
from ardef.persistence.file import FileStorage

log = Logger(__name__)
fs = FileStorage()
 
_MATCH_KEYS = ["table_key", "low_key_for_range", "delete_indicator"]
_EFFECTIVE_DATE_COL = "effective_date"
_DATE_VALID_COL = "valid_until"
_LINES_COL = "lines"
_LU_ARDEF_FILENAME = "data.parquet"
_DATE_FORMATS = ["%Y%m%d", "%Y-%m-%d", "%d%m%Y"]
# Nombre de la columna temporal interna para la fecha parseada de effective_date.
# Se usa _eff_parsed (no _eff_ts) para evitar colisión con el campo persistente
# _eff_ts que viaja en el DataFrame desde interpreter.py.
_EFF_PARSED_COL = "_eff_parsed"
 
# Columnas de lu_ardef necesarias para la lógica de cálculo.
# Se cargan en la Fase 1 (pandas, liviano) para evitar cargar las 54 columnas en memoria.
# La Fase 2 carga el full con Arrow (eficiente) solo para escribir.
_LU_LOGIC_COLS = [_LINES_COL] + _MATCH_KEYS + [_EFFECTIVE_DATE_COL, _DATE_VALID_COL]
 
 
def _parse_effective_date_series(series: pd.Series) -> pd.Series:
    """
    Convierte una Serie de cadenas a pd.Timestamp probando múltiples formatos
    (`_DATE_FORMATS`). Devuelve NaT cuando no es posible parsear.

    Args:
        series: Serie de strings a parsear (típicamente `effective_date`).

    Returns:
        Serie de pd.Timestamp (NaT donde no se pudo parsear).

    Ejemplo:
        _parse_effective_date_series(pd.Series(["20260424"]))
        # -> [Timestamp('2026-04-24')]
    """

    def _try(val: str) -> Optional[pd.Timestamp]:
        if pd.isna(val) or str(val).strip() in ("", "nan", "NaT", "None"):
            return pd.NaT
        for fmt in _DATE_FORMATS:
            try:
                return pd.Timestamp(datetime.strptime(str(val).strip(), fmt).date())
            except ValueError:
                continue
        return pd.NaT
    
    return series.apply(_try)
 
 
def _valid_until_to_str(series: pd.Series) -> pd.Series:
    """
    Convierte una serie de fechas (datetime64 / Timestamp / string) a strings
    'yyyy-mm-dd' con pd.NA para nulos (registros vigentes).

    Se usa pd.StringDtype() para que pyarrow escriba siempre como utf8
    y nunca como date32 (int32), independientemente del contenido.

    Args:
        series: Serie con fechas en cualquier formato aceptado por
            `pd.to_datetime` (datetime64, Timestamp o string), o valores
            nulos para registros aún vigentes.

    Returns:
        Serie de tipo `pd.StringDtype()` con fechas "YYYY-MM-DD" o `pd.NA`.

    Ejemplo:
        _valid_until_to_str(pd.Series([pd.Timestamp("2026-04-24"), None]))
        # -> ["2026-04-24", <NA>]
    """
    dt = pd.to_datetime(series, errors="coerce")                         # datetime64[ns]
    result = dt.dt.strftime("%Y-%m-%d").where(dt.notna(), other=pd.NA)   # str o pd.NA
    return result.astype(pd.StringDtype())                                # fuerza utf8


def _load_lu_ardef_logic(filepath: str) -> pd.DataFrame:
    """
    Carga solo las columnas necesarias para el cálculo desde lu_ardef (Fase 1).

    Carga únicamente _LU_LOGIC_COLS (6 columnas) en lugar de las 54 del full.
    Esto reduce el uso de RAM de ~5.5 GB a ~0.3 GB para 1.7M filas.

    Retorna DataFrame vacío si la key no existe (primera ejecución).

    Args:
        filepath: Ruta/key de lu_ardef dentro de la capa REFERENCE (ver
            `FileStorage.get_lu_ardef_filepath`).

    Returns:
        DataFrame con las columnas `_LU_LOGIC_COLS` (agrega `valid_until`
        como NaT si el parquet leído no la tenía). DataFrame vacío si el
        archivo no existe (primera ejecución) o si ocurre cualquier otro
        error de lectura (logueado como error, no se propaga la excepción).

    Ejemplo:
        _load_lu_ardef_logic("reference/visa/ardef/lu_ardef/data.parquet")
    """
    try:
        df = fs.read_parquet_by_filepath(
            filepath,
            layer=FileStorage.Layer.REFERENCE,
            columns=_LU_LOGIC_COLS,
        )
 
        if _DATE_VALID_COL not in df.columns:
            df[_DATE_VALID_COL] = pd.NaT
 
        log.logger.info(f"lu_ardef (logic cols) cargado: {len(df)} filas | key={filepath}")
        return df
 
    except FileNotFoundError:
        log.logger.info(
            f"lu_ardef no encontrado en key={filepath}. "
            f"Se asume primera ejecución."
        )
        return pd.DataFrame()
 
    except Exception as exc:
        log.logger.error(f"Error al leer lu_ardef logic ({filepath}): {exc}")
        return pd.DataFrame()


def _deduplicate_incoming(
    df: pd.DataFrame,
    lu_ardef: pd.DataFrame,
    file_id: str,
    file_processing_date: str, 
) -> pd.DataFrame:
    """
    Elimina de las filas entrantes aquellas cuyo campo 'lines' ya existe en lu_ardef.

    El campo 'lines' es la línea de texto original del archivo fuente, y actúa como
    llave natural única de cada registro: si 'lines' ya esta en lu_ardef, el registro
    es idéntico al que procesó en una ejecución anterior -> se descarta.

    Args:
        df: DataFrame CLEAN (filas entrantes candidatas a nuevas).
        lu_ardef: Tabla maestra lu_ardef ya cargada (columnas de lógica),
            usada solo para consultar el set de 'lines' ya existentes.
        file_id: ID único del archivo, para logging.
        file_processing_date: Fecha de negocio del archivo, para logging.

    Returns:
        Subconjunto de `df` sin las filas cuyo 'lines' ya está en
        `lu_ardef` (índice reseteado). Si `lu_ardef` está vacío o no tiene
        la columna 'lines', retorna `df` sin cambios.

    Ejemplo:
        _deduplicate_incoming(df, lu_ardef, "ABC123", "2026-04-24")
    """
    if lu_ardef.empty:
        return df
    
    if _LINES_COL not in lu_ardef.columns or _LINES_COL not in df.columns:
        log.logger.warning(
            f"Columna {_LINES_COL} ausente; se omite deduplicación | "
            f"file_id={file_id}, file_processing_date={file_processing_date}"
        )
        return df
    
    existing_lines: set = set(lu_ardef[_LINES_COL].dropna().str.rstrip())
    duplicate_mask = df[_LINES_COL].str.rstrip().isin(existing_lines)
    n_duplicates = int(duplicate_mask.sum())
 
    if n_duplicates > 0:
        log.logger.info(
            f"{n_duplicates} registro(s) duplicado(s) ignorados "
            f"(lines ya presente en data) | "
            f"file_id={file_id}, file_processing_date={file_processing_date}"
        )
 
    return df[~duplicate_mask].reset_index(drop=True)
    

def _build_calculate_dataframe(
        clean: pd.DataFrame,
        lu_logic: pd.DataFrame,
        file_id: str,
        file_processing_date: str,
) -> tuple[pd.DataFrame, pd.Series | None]:
    """
    Calcula valid_until para los registros nuevos usando solo las columnas de lógica.
 
    Flujo:
    Paso 0: Deduplicación por 'lines':
        Registros cuyo campo 'lines' ya existe en lu_logic son idénticos a los ya 
        procesados -> se descartan antes de cualquier otra lógica.
 
    Paso 1: Actualizar registros ANTERIORES en lu_logic
        Para cada fila nueva con effective_date E, los registros de lu con las mismas 
        claves y _eff_parsed < E reciben: 
            valid_until = min(valid_until_actual, E - 1d)
 
    Paso 2: valid_until para filas nuevas out-of-order:
        Si en lu existe un registro con las mismas claves y _eff_parsed > E:
            valid_until_nueva = min(eff_sucesor_en_lu) - 1d

    Args:
        clean: DataFrame CLEAN (etapa 300_ARDEF_CLN), con todos los campos ya
            casteados.
        lu_logic: Tabla maestra lu_ardef con solo las columnas de lógica
            (`_LU_LOGIC_COLS`), tal como la devuelve `_load_lu_ardef_logic`.
            Vacía en la primera ejecución.
        file_id: ID único del archivo, para logging.
        file_processing_date: Fecha de negocio del archivo, para logging.

    Returns:
        Tupla (df_calculate, lu_valid_until):
        * df_calculate: nuevas filas con valid_until calculado (400_ARDEF_CAL).
                        Contiene todas las columnas del CLEAN + valid_until.
        * lu_valid_until: Serie con el valid_until actualizado para TODAS las filas de
                          lu_logic, indexed 0..N-1 (mismo orden posicional que lu_full).
                          None si lu_logic estaba vacío (primera ejecución) o si todos
                          los registros entrantes eran duplicados (sin cambios en la maestra).

    Ejemplo:
        _build_calculate_dataframe(clean, lu_logic, "ABC123", "2026-04-24")
    """
 
    # nuevas filas con valid_until inicializado a NaT
    df = clean.copy()
    df[_DATE_VALID_COL] = pd.NaT
 
    # DataFrame CLEAN vacío
    if clean.empty:
        log.logger.warning(
            f"CLEAN parquet vacío para file_id={file_id}, "
            f"file_processing_date={file_processing_date}"
        )
        return df, None
 
    # Paso 0: Deduplicar por 'lines'
    df = _deduplicate_incoming(
        df=df,
        lu_ardef=lu_logic,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )
 
    # Si todas las filas eran duplicadas -> lu_ardef sin cambios, CAL vacío
    if df.empty:
        log.logger.info(
            f"Todos los registros del archivo son duplicados de lu_ardef. "
            f"Sin cambios en la maestra | "
            f"file_id={file_id}, file_processing_date={file_processing_date}"
        )
        empty_cal = pd.DataFrame(columns=clean.columns.tolist() + [_DATE_VALID_COL])
        return empty_cal, None
 
    # Parsear effective_date de las nuevas filas como Timestamp (columna temporal interna)
    # Se usa _eff_parsed, NO _eff_ts, para no colisionar con el campo persistente _eff_ts
    # que viene desde interpreter.py y viaja por todo el pipeline.
    df[_EFF_PARSED_COL] = _parse_effective_date_series(df[_EFFECTIVE_DATE_COL].astype(str))
 
    # Primera ejecución: lu_logic vacío
    if lu_logic.empty:
        log.logger.info(
            "lu_ardef vacío (primera ejecución). "
            "Todas las filas nuevas quedan con valid_until = None (vigentes)"
        )
        df_out = df.drop(columns=[_EFF_PARSED_COL])
        df_out[_DATE_VALID_COL] = _valid_until_to_str(df_out[_DATE_VALID_COL])
        # None indica que no hay lu_full previo; calculate_ardef usará df_out como nueva maestra
        return df_out, None
 
    # Ejecución con lu_logic existente
    log.logger.info(
        f"Aplicando lógica de valid_until contra lu_ardef ({len(lu_logic)} filas) "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )
 
    lu = lu_logic.copy()
 
    # Convertir valid_until de string a Timestamp para la aritmética interna
    lu[_DATE_VALID_COL] = pd.to_datetime(lu[_DATE_VALID_COL], errors="coerce")
 
    lu[_EFF_PARSED_COL] = _parse_effective_date_series(lu[_EFFECTIVE_DATE_COL].astype(str))
    lu = lu.reset_index(drop=True)
    lu["_lu_idx"] = lu.index
 
    # Tablas de claves para los JOINS
    df_keys = df[_MATCH_KEYS + [_EFF_PARSED_COL]].rename(columns={_EFF_PARSED_COL: "_new_eff_ts"})
    lu_keys = lu[_MATCH_KEYS + ["_lu_idx", _EFF_PARSED_COL, _DATE_VALID_COL]]
 
    # Paso 1 - Actualizar registros anteriores en lu_ardef
    merged_prev = lu_keys.merge(df_keys, on=_MATCH_KEYS, how="left")
    merged_prev = merged_prev[merged_prev["_new_eff_ts"] > merged_prev[_EFF_PARSED_COL]]
 
    lu_updated_count = 0
    if not merged_prev.empty:
        min_new_eff = (
            merged_prev
            .groupby("_lu_idx")["_new_eff_ts"]
            .min()
            .rename("_candidate_dv")
        )
        lu = lu.join(min_new_eff, on="_lu_idx")
 
        has_cand = lu["_candidate_dv"].notna()
        cand_minus1 = lu.loc[has_cand, "_candidate_dv"] - pd.Timedelta(days=1)
        curr = lu.loc[has_cand, _DATE_VALID_COL]
 
        lu.loc[has_cand, _DATE_VALID_COL] = (
            pd.concat([curr, cand_minus1], axis=1).min(axis=1)
        )
        lu_updated_count = int(has_cand.sum())
        lu = lu.drop(columns=["_candidate_dv"])
 
    # Paso 2 - valid_until para filas nuevas out-of-order
    df = df.reset_index(drop=True)
    df["_df_idx"] = df.index
 
    lu_succ = lu[_MATCH_KEYS + [_EFF_PARSED_COL]].rename(columns={_EFF_PARSED_COL: "_lu_eff_ts"})
    merged_next = df[_MATCH_KEYS + ["_df_idx", _EFF_PARSED_COL]].merge(
        lu_succ, on=_MATCH_KEYS, how="left"
    )
    merged_next = merged_next[merged_next["_lu_eff_ts"] > merged_next[_EFF_PARSED_COL]]
 
    new_rows_with_dv = 0
    if not merged_next.empty:
        min_lu_succ = (
            merged_next
            .groupby("_df_idx")["_lu_eff_ts"]
            .min()
            .rename("_candidate_dv")
        )
        df = df.join(min_lu_succ, on="_df_idx")
 
        has_cand = df["_candidate_dv"].notna()
        cand_minus1 = df.loc[has_cand, "_candidate_dv"] - pd.Timedelta(days=1)
        curr = df.loc[has_cand, _DATE_VALID_COL]
 
        df.loc[has_cand, _DATE_VALID_COL] = (
            pd.concat([curr, cand_minus1], axis=1).min(axis=1)
        )
        new_rows_with_dv = int(has_cand.sum())
        df = df.drop(columns=["_candidate_dv"])
 
    log.logger.info(
        f"{lu_updated_count} fila(s) de lu_ardef actualizadas con valid_until | "
        f"{new_rows_with_dv} fila(s) nuevas recibieron valid_until por out-of-order | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )
 
    # Limpiar columnas temporales internas
    lu = lu.drop(columns=[_EFF_PARSED_COL, "_lu_idx"])
    df_out = df.drop(columns=[_EFF_PARSED_COL, "_df_idx"])
 
    # Convertir valid_until a string 'yyyy-mm-dd' (pd.NA para vigentes)
    lu[_DATE_VALID_COL] = _valid_until_to_str(lu[_DATE_VALID_COL])
    df_out[_DATE_VALID_COL] = _valid_until_to_str(df_out[_DATE_VALID_COL])
 
    # Retornar df_out + la serie valid_until de lu (indexed 0..N-1)
    # calculate_ardef la aplicará sobre lu_full (Arrow) sin necesidad de tenerlos
    # ambos en memoria al mismo tiempo.
    return df_out, lu[_DATE_VALID_COL]
 
 
# Función pública del módulo
 
def calculate_ardef(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    file_id: str,
    file_processing_date: str,
    origin_subdir: str = "300_ARDEF_CLN",
    target_subdir: str = "400_ARDEF_CAL",
    operational_subdir: str = _LU_ARDEF_FILENAME,
) -> None:
    """
    Etapa CALCULATE del pipeline ARDEF.
 
    Estrategia de memoria en dos fases para manejar lu_ardef de millones de filas:
 
    Fase 1 — Cálculo liviano (pandas, ~0.5 GB):
        - Leer CLEAN (609k filas, 54 cols)
        - Leer lu_ardef con solo 6 columnas de lógica (_LU_LOGIC_COLS)
        - Calcular valid_until: dedup, Paso 1, Paso 2
        - Resultado: df_calculate (nuevas filas) + lu_valid_until (Serie de actualizaciones)
        - Liberar clean y lu_logic de memoria (gc.collect)
 
    Fase 2 — Escritura eficiente (PyArrow, ~0.75 GB):
        - Leer lu_ardef completo (54 cols) como PyArrow Table
        - Aplicar actualizaciones de valid_until reemplazando solo esa columna
        - Concatenar las nuevas filas
        - Escribir lu_ardef actualizado a REFERENCE
 
    Pasos:
    1. Leer STAGING / 300_ARDEF_CLN
    2. Leer lu_ardef (solo columnas de lógica) desde REFERENCE
    3. Calcular valid_until con soporte out-of-order
    4. Liberar memoria (Fase 1 → Fase 2)
    5. Leer lu_ardef completo como Arrow, actualizar y escribir en REFERENCE
    6. Escribir STAGING / 400_ARDEF_CAL (solo nuevas filas)

    valid_until se almacena siempre como string 'yyyy-mm-dd' o None.

    Args:
        origin_layer: Layer de origen del CLEAN (típicamente STAGING).
        target_layer: Layer de destino del CALCULATE (típicamente STAGING).
        file_id: ID único del archivo.
        file_processing_date: Fecha de negocio del archivo, "YYYY-MM-DD".
        origin_subdir: Subdirectorio de origen (default "300_ARDEF_CLN").
        target_subdir: Subdirectorio de destino en STAGING (default
            "400_ARDEF_CAL").
        operational_subdir: Nombre de archivo de la tabla maestra dentro de
            REFERENCE (default "data.parquet", ver `_LU_ARDEF_FILENAME`).

    Returns:
        None. Actualiza lu_ardef en REFERENCE y escribe el parquet CALCULATE
        (solo filas nuevas) en STAGING, ambos como efecto secundario.
        Cualquier error al escribir lu_ardef se loguea pero no interrumpe el
        procesamiento — el parquet CALCULATE se escribe igualmente.

    Ejemplo:
        calculate_ardef(FileStorage.Layer.STAGING, FileStorage.Layer.STAGING,
                         "ABC123", "2026-04-24")
    """
 
    log.logger.info(
        f"Inicio calculate_ardef | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )
 
    # ── Fase 1: cálculo liviano ────────────────────────────────────────────
 
    # 1. Leer CLEAN
    clean = fs.read_parquet(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir,
    )
 
    log.logger.info(
        f"CLEAN leido: {len(clean)} filas | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )
 
    # 2. Leer lu_ardef con solo las columnas de lógica
    lu_ardef_filepath = fs.get_lu_ardef_filepath()
 
    log.logger.info(f"lu_ardef filepath: {lu_ardef_filepath}")
 
    lu_logic = _load_lu_ardef_logic(lu_ardef_filepath)
    is_first_execution = lu_logic.empty
 
    # 3. Calcular valid_until
    df_calculate, lu_valid_until = _build_calculate_dataframe(
        clean=clean,
        lu_logic=lu_logic,
        file_id=file_id,
        file_processing_date=file_processing_date,
    )
 
    # 4. Liberar Fase 1 antes de cargar lu_full en Fase 2
    del clean, lu_logic
    gc.collect()
 
    log.logger.info(
        f"Fase 1 completada | df_calculate={len(df_calculate)} filas nuevas | "
        f"lu_valid_until={'actualizada' if lu_valid_until is not None else 'sin cambios'}"
    )
 
    # ── Fase 2: escritura eficiente con Arrow ──────────────────────────────
 
    # 5. Actualizar y escribir lu_ardef
    try:
        if is_first_execution:
            # Primera ejecución: df_calculate ES la nueva maestra, escribir directo
            if not df_calculate.empty:
                fs.write_parquet_by_filepath(
                    data=df_calculate,
                    filepath=lu_ardef_filepath,
                    index=False,
                    layer=FileStorage.Layer.REFERENCE,
                )
                log.logger.info(
                    f"lu_ardef creado (primera ejecución): {len(df_calculate)} filas | "
                    f"key={lu_ardef_filepath}"
                )
            else:
                log.logger.warning(
                    f"Primera ejecución con CLEAN vacío. lu_ardef no creado."
                )
 
        elif lu_valid_until is not None or not df_calculate.empty:
            # Ejecuciones normales: cargar lu_full como Arrow (eficiente en memoria),
            # aplicar actualizaciones de valid_until y concatenar nuevas filas.
            lu_arrow = fs.read_arrow_by_filepath(
                lu_ardef_filepath,
                layer=FileStorage.Layer.REFERENCE,
            )
 
            log.logger.info(
                f"lu_ardef (full Arrow) cargado: {lu_arrow.num_rows} filas | "
                f"key={lu_ardef_filepath}"
            )
 
            # Reemplazar columna valid_until con los valores actualizados
            if lu_valid_until is not None:
                col_idx = lu_arrow.schema.get_field_index(_DATE_VALID_COL)
                original_type = lu_arrow.schema.field(_DATE_VALID_COL).type
                new_col = pa.array(lu_valid_until, from_pandas=True, type=original_type)
 
                if col_idx >= 0:
                    lu_arrow = lu_arrow.set_column(col_idx, _DATE_VALID_COL, new_col)
                else:
                    lu_arrow = lu_arrow.append_column(_DATE_VALID_COL, new_col)
 
            # Concatenar nuevas filas
            if not df_calculate.empty:
                new_rows_arrow = pa.Table.from_pandas(df_calculate, preserve_index=False)
                # Reordenar columnas para que coincidan con lu_arrow antes de concatenar
                new_rows_arrow = new_rows_arrow.select(lu_arrow.schema.names)
                lu_arrow = pa.concat_tables(
                    [lu_arrow, new_rows_arrow],
                    promote_options="default",
                )
 
            fs.write_arrow_by_filepath(
                table=lu_arrow,
                filepath=lu_ardef_filepath,
                layer=FileStorage.Layer.REFERENCE,
            )
 
            log.logger.info(
                f"lu_ardef actualizado en REFERENCE: {lu_ardef_filepath} "
                f"({lu_arrow.num_rows} filas totales)"
            )
 
            del lu_arrow
            gc.collect()
 
        else:
            log.logger.info(
                f"Sin cambios en lu_ardef (todos duplicados) | "
                f"file_id={file_id}, file_processing_date={file_processing_date}"
            )
 
    except Exception as exc:
        log.logger.error(
            f"Error al escribir lu_ardef.parquet ({lu_ardef_filepath}): {exc}"
        )
 
    # 6. Escribir calculate (solo nuevas filas)
    output_filepath = fs.write_parquet(
        data=df_calculate,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False,
    )
 
    log.logger.info(
        f"ARDEF CALCULATE parquet creado exitosamente: {output_filepath}"
    )