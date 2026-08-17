"""
handler.py — Lambda real: itl-0004-itx-dev-intchg-02-lmbd-rules-refresh
================================================================================
Archivo:     lambdas/rules-refresh/src/handler.py

Refresca visa_rules/data.parquet y mc_rules/data.parquet en s3-reference a
partir de los excels de reglas de interchange que el negocio sube
periodicamente (mismo insumo que legacy leia de forma manual desde
Intelica/INTERCHANGE_RULES/{VISA,MASTERCARD}/... — ver InterchangeRules.py).
Reemplaza el proceso manual (tst_files/interchange_rules/build_and_compare_rules.py)
por un trigger S3 real, con las mismas validaciones ya probadas contra los
excels V37 (Visa)/V23 (Mastercard), mas 3 guardas nuevas que el script manual
no tenia (ver mas abajo).

Trigger: evento S3 ObjectCreated en s3-reference, prefijo
"interchange_rules/{VISA|MASTERCARD}/" (filtro de sufijo .xlsx/.xls
recomendado en la notificacion S3, no es estrictamente necesario porque el
handler tambien lo valida).

Flujo por archivo:
  1. Detectar marca por el subprefijo (VISA o MASTERCARD bajo interchange_rules/).
  2. Ignorar cualquier key bajo un subpath que empiece con "_" (_archive/,
     _rejected/, _reports/) — son destinos propios de este mismo Lambda, no
     hay que reprocesarlos si la notificacion S3 esta filtrada solo por
     prefijo (sin filtro de sufijo estricto, estos tambien matchearian).
  3. Descargar el excel completo (son archivos chicos, a diferencia de los
     archivos transaccionales — no hace falta streaming).
  4. Validar estructura (replica read_rules_visa/read_rules_mc del legacy,
     ya probada contra V37/V23) + 3 guardas nuevas:
       - Nulls en columnas requeridas -> falla dura (legacy solo lo logueaba
         como warning). Excepcion: en MC, CATEGORY y FEE_TIER tienen fillna()
         propio aguas abajo (fee_category, sentinel "S/I") — nulls ahi son
         esperados, no error.
       - Cast numerico que no parsea -> falla dura (antes: warning). Para
         RATE_VARIABLE de MC (no se castea/almacena en esta etapa) solo
         aplica sobre reglas TODAVIA VIGENTES (ver _active_rule_mask) — el
         excel real V23 ya trae 28 filas de Bahamas con "1.75%" en reglas
         YA VENCIDAS (ver gotchas.md); sin este matiz, cualquier excel
         futuro heredaria ese mismo dato sucio historico y el refresh
         fallaria siempre, aunque el dato nunca se use.
       - Ratio de filas nuevo/actual por debajo de RULES_MIN_ROW_RATIO
         (default 0.5) -> falla dura. Guarda barata contra "subieron el
         sheet equivocado" o un archivo truncado.
       - VISA: TRANSACTION_CODE en notacion abreviada de 1 digito (formato
         V38+, ej. "5") se expande a la familia completa de TCs (ver
         VISA_TC_FAMILIES/_expand_transaction_code_family) antes de
         guardar — codigos ya completos de 2 digitos (formato V37, ej.
         "25") se dejan sin tocar.
       - Error al leer/parsear el excel (hoja no encontrada, archivo
         corrupto) -> tratado igual que un RulesValidationError, no como
         error de infraestructura (sin este guard, un archivo con la hoja
         equivocada quedaba atascado sin moverse a _rejected/ ni _archive/).
  5. Si falla: mover el excel a interchange_rules/{marca}/_rejected/, loguear
     FAILED, listo — NO se toca visa_rules/mc_rules.
  6. Si valida OK: calcular diff contra el parquet actual (altas/bajas/
     columnas con valores distintos, mismo criterio que compare() del
     prototipo), respaldar el parquet actual a
     {visa_rules|mc_rules}/history/data_{timestamp}.parquet, escribir el
     nuevo data.parquet, mover el excel a interchange_rules/{marca}/_archive/,
     loguear SUCCESS con el resumen del diff.

Por que las fallas de validacion NO relanzan la excepcion (solo los errores
inesperados de infraestructura si): un excel mal formado es un "poison pill"
— reintentarlo (el retry automatico de invocaciones asincronas S3->Lambda)
no lo arregla, solo genera ruido. Un fallo de S3 en cambio puede ser
transitorio y vale la pena que Lambda lo reintente.

Validado localmente (2026-08-03) contra los 2 excels reales del repo
(tst_files/interchange_rules/VISA Reglas Intercambio V37.xlsx,
MASTERCARD Reglas Intercambio V23.xlsx): _build_visa_rules()/_build_mc_rules()
producen un DataFrame identico (assert_frame_equal, check_dtype=False) al
generado por el prototipo build_and_compare_rules.py para ambas marcas — no
se solo probo que "no rompe", se confirmo equivalencia fila a fila.

Uso real en produccion (no solo pruebas):
  - 2026-08-10: primera prueba end-to-end sobre lmbd-test-1 con el trigger S3
    real (subida de un excel real disparando el Lambda sin invocacion
    manual).
  - 2026-08-11: primer refresh real de un cambio de negocio genuino (excel
    VISA V38, expansion de familias de TRANSACTION_CODE — ver
    VISA_TC_FAMILIES/_expand_transaction_code_family arriba) — SUCCESS,
    backup automatico a visa_rules/history/, visa_rules/data.parquet
    publicado, smoke test de glue-vi-interchange contra 2 archivos reales
    (EBGR/SBSA) sin regresion. Detalle completo en decisions.md.
  - 2026-08-17: migrado de lmbd-test-1 (infra prestada) a este Lambda
    definitivo (creado por el equipo de infra) — mismo rol/layers/config,
    trigger S3 reapuntado. Validado con el mismo excel V38 ya archivado
    (diff=0 esperado y confirmado). Detalle completo en decisions.md.

Variables de entorno:
  S3_BUCKET_REFERENCE            : bucket s3-reference (incoming, parquet, history, archive)
  RULES_MIN_ROW_RATIO            : ratio minimo filas_nuevas/filas_actuales antes de rechazar (default: 0.5)
"""

import os
import json
import logging
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

BUCKET_REFERENCE = os.environ.get('S3_BUCKET_REFERENCE')
MIN_ROW_RATIO = float(os.environ.get('RULES_MIN_ROW_RATIO', '0.5'))

INCOMING_ROOT = "interchange_rules"


class RulesValidationError(Exception):
    """Excel de reglas mal formado o con contenido invalido — no es un error
    de infraestructura, no debe reintentarse automaticamente."""
    pass


# =============================================================================
# CONFIGURACION POR MARCA
# =============================================================================

VISA_REQUIRED = [
    "JURISDICTION", "GUIDE_DATE", "VALID_FROM", "FEE_PROGRAM",
    "INTELICA_ID", "FPI", "FEE_DESCRIPTOR", "FEE_DESCRIPTION", "COD_HIERARCHY",
]
VISA_NUMERIC = ["FEE_FIXED", "FEE_MIN", "FEE_CAP", "FEE_VARIABLE"]
VISA_KEY_COLS = ["jurisdiction", "guide_date", "valid_from", "intelica_id"]

MC_SELECT_COLS = [
    "JURISDICTION", "REGION_COUNTRY_CODE", "GUIDE_DATE", "VALID_FROM", "VALID_UNTIL",
    "CATEGORY", "PAYMENT_PRODUCT", "FEE_TIER", "INTELICA_ID", "IRD", "RATE_CURRENCY",
    "RATE_VARIABLE", "RATE_FIXED", "RATE_MIN", "RATE_CAP", "PROCESSING_CODE",
    "AMOUNT_TRANSACTION_CURRENCY", "AMOUNT_TRANSACTION", "CARD_PRESENT_DATA",
    "CARD_ACCEPTOR_BUSINESS_CODE", "MERCHANT_COUNTRY", "TRANSACTION_DESTINATION_INSTITUTION",
    "ISSUER_COUNTRY", "CARD_PROGRAM_INDICATOR", "GCMS_PRODUCT_IDENTIFIER", "FUNDING_SOURCE",
    "TTI", "UCAF_COLLECTION_ID", "TOKEN_FLAG", "MASTERCARD_ASSIGNED_ID", "ISSUER_BIN_8",
    "MASTERPASS_INCENTIVE_INDICATOR", "ADDITIONAL_DATA",
]
MC_RENAME_FINAL = [
    "jurisdiction", "region_country_code", "guide_date", "valid_from", "valid_until",
    "fee_category", "fee_tier", "intelica_id", "ird", "rate_currency", "rate_variable",
    "rate_fixed", "rate_min", "rate_cap", "processing_code", "amount_transaction_currency",
    "amount_transaction", "card_present_data", "card_acceptor_business_code", "merchant_country",
    "transaction_destination_institution", "issuer_country", "card_program_indicator",
    "gcms_product_identifier", "funding_source", "tti", "ucaf_collection_id", "token_flag",
    "mastercard_assigned_id", "issuer_bin_8", "masterpass_incentive_indicator", "additional_data",
]
# Nulls jamas tolerables — si aparecen, el excel esta mal formado.
MC_REQUIRED_HARD = ["JURISDICTION", "GUIDE_DATE", "VALID_FROM", "INTELICA_ID", "IRD"]
# Nulls tolerados — read_rules_mc() los rellena a proposito (fee_category via
# fillna(""), fee_tier via fillna("S/I")). Solo se loguean, no fallan.
MC_REQUIRED_SOFT = ["CATEGORY", "FEE_TIER"]
MC_NUMERIC = ["RATE_FIXED", "RATE_MIN", "RATE_CAP"]
# No se castea/almacena (igual que legacy la deja como texto), pero se valida
# que parsee en reglas vigentes — caso real ya visto: "1.75%" en vez de
# 0.0175 en filas ya vencidas (ver gotchas.md / _active_rule_mask).
MC_NUMERIC_SOFT_CHECK = ["RATE_VARIABLE"]
MC_KEY_COLS = ["jurisdiction", "guide_date", "valid_from", "intelica_id"]

DATE_LIKE_COLS = {"guide_date", "valid_from", "valid_until"}
NA_STRINGS = {"", "nan", "none", "nat", "<na>"}

# A partir de la version V38 del excel VISA, TRANSACTION_CODE llega como un
# digito base sin cero a la izquierda (ej. "5", "5,6") en vez de la lista
# explicita de codigos que traia V37 (ej. "05,25"). Un codigo base es la
# notacion abreviada de toda una FAMILIA de TCs que comparten el mismo
# business_transaction_type (ver calc_business_transaction_type_draft en
# glue/scripts/visa/calculate/calculate.py: purchase_codes/cash_codes/
# atm_codes) — 05 (compra 1ra presentacion) implica tambien su reversal
# 15/35 y su contracargo 25.
VISA_TC_FAMILIES = {
    "05": ("05", "15", "25", "35"),
    "06": ("06", "16", "26", "36"),
    "07": ("07", "17", "27", "37"),
}


def _expand_transaction_code_family(value):
    """
    Expande un valor de TRANSACTION_CODE a su familia completa cuando viene
    en notacion abreviada de 1 digito (formato V38+). Un codigo que YA
    viene con 2 digitos (formato V37, ej. "25") se deja tal cual, sin
    expandir — ahi el excel ya listaba el codigo exacto que la regla
    necesitaba, y expandirlo igual romperia reglas viejas que a proposito
    no incluian codigos de reversal/re-presentment.

    Args:
        value: Valor crudo de la celda TRANSACTION_CODE (ej. "5,6", "05,25"
            o NaN/None si la regla no tiene esta condicion).

    Returns:
        String con los codigos expandidos y deduplicados, separados por
        coma, en el mismo formato que ya consume `_apply_default()` en
        `glue/scripts/visa/interchange/interchange.py`. Devuelve `value`
        sin cambios si es nulo/vacio.

    Ejemplo:
        _expand_transaction_code_family("5,6")
        # -> "05,15,25,35,06,16,26,36"
        _expand_transaction_code_family("05,25")
        # -> "05,25" (formato V37, sin cambios)
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    text = str(value).strip()
    if text == "" or text.upper() in ("NAN", "NONE"):
        return value

    codes = [c.strip() for c in text.split(",") if c.strip() != ""]
    expanded = []
    seen = set()
    for code in codes:
        if code.isdigit() and len(code) == 1:
            family = VISA_TC_FAMILIES.get(code.zfill(2), (code.zfill(2),))
        else:
            family = (code,)
        for fc in family:
            if fc not in seen:
                seen.add(fc)
                expanded.append(fc)
    return ",".join(expanded)

BRAND_CONFIG = {
    "VISA": {
        "sheet_name": "Visa",
        "skiprows": 3,
        "rules_key": "visa_rules/data.parquet",
        "history_prefix": "visa_rules/history/",
        "incoming_prefix": f"{INCOMING_ROOT}/VISA/",
        "key_cols": VISA_KEY_COLS,
    },
    "MASTERCARD": {
        "sheet_name": "Mastercard ITX & Service",
        "skiprows": 1,
        "rules_key": "mc_rules/data.parquet",
        "history_prefix": "mc_rules/history/",
        "incoming_prefix": f"{INCOMING_ROOT}/MASTERCARD/",
        "key_cols": MC_KEY_COLS,
    },
}


def _validar_configuracion() -> None:
    """
    Verifica que las variables de entorno requeridas esten configuradas.

    Returns:
        None. Lanza ValueError si falta S3_BUCKET_REFERENCE.
    """
    if not BUCKET_REFERENCE:
        raise ValueError("Falta variable de entorno requerida: S3_BUCKET_REFERENCE")


# =============================================================================
# DETECCION DE MARCA / RUTAS ADMINISTRATIVAS
# =============================================================================

def _detect_brand(key: str) -> Optional[str]:
    """
    Detecta la marca (VISA/MASTERCARD) a partir del subprefijo del key,
    dentro de interchange_rules/{VISA|MASTERCARD}/.

    Args:
        key: S3 key completo del objeto.

    Returns:
        "VISA" o "MASTERCARD" si el key cae bajo un prefijo reconocido en
        BRAND_CONFIG, o None si no matchea ninguno (archivo fuera de scope
        para este Lambda).

    Ejemplo:
        _detect_brand("interchange_rules/VISA/VISA Reglas Intercambio V38.xlsx")
        # -> "VISA"
    """
    parts = key.split('/')
    if len(parts) < 3 or parts[0] != INCOMING_ROOT:
        return None
    brand = parts[1].upper()
    return brand if brand in BRAND_CONFIG else None


def _is_admin_subpath(key: str) -> bool:
    """
    Detecta si el key cae bajo un subprefijo administrativo propio de este
    Lambda (_archive/, _rejected/, _reports/) — hay que ignorarlos si la
    notificacion S3 esta filtrada solo por prefijo (sin filtro de sufijo
    estricto), porque de otro modo el propio Lambda se re-dispararia sobre
    los archivos que el mismo escribe.

    Args:
        key: S3 key completo del objeto.

    Returns:
        True si algun segmento del path empieza con "_".

    Ejemplo:
        _is_admin_subpath("interchange_rules/VISA/_archive/20260803_x.xlsx")
        # -> True
    """
    return any(part.startswith('_') for part in key.split('/'))


# =============================================================================
# VALIDACION Y CONSTRUCCION — VISA
# =============================================================================

def _check_required_columns(df: pd.DataFrame, required_cols: List[str], label: str) -> None:
    """
    Falla dura si falta alguna columna requerida en el excel.

    Args:
        df: DataFrame crudo leido del excel.
        required_cols: Columnas que deben existir.
        label: "VISA" o "MASTERCARD", solo para el mensaje de error.

    Returns:
        None.

    Raises:
        RulesValidationError: si falta alguna columna.
    """
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RulesValidationError(f"{label}: faltan columnas requeridas en el excel: {missing}")


def _check_required_nulls(df: pd.DataFrame, required_cols: List[str], label: str) -> None:
    """
    Falla dura si alguna columna requerida (de las que NO tienen un fillna
    intencional aguas abajo) tiene valores nulos.

    Args:
        df: DataFrame a chequear.
        required_cols: Columnas que nunca deberian ser null.
        label: "VISA" o "MASTERCARD", solo para el mensaje de error.

    Returns:
        None.

    Raises:
        RulesValidationError: si alguna columna requerida tiene nulls.
    """
    problems = []
    for col in required_cols:
        if col not in df.columns:
            continue
        n_null = int(df[col].isnull().sum())
        if n_null:
            problems.append(f"{col} ({n_null} filas)")
    if problems:
        raise RulesValidationError(
            f"{label}: columnas requeridas con valores nulos — {', '.join(problems)}. "
            "El legacy abortaria la carga completa ante esto."
        )


def _check_numeric_parseable(
    df: pd.DataFrame, cols: List[str], label: str, mask: Optional[pd.Series] = None
) -> None:
    """
    Falla dura si alguna columna numerica tiene valores no vacios que no
    parsean como float (ej. "1.75%" en vez de 0.0175). Sin este chequeo,
    Spark aguas abajo convertiria el valor a NULL en silencio.

    Args:
        df: DataFrame a chequear (columnas todavia como string).
        cols: Columnas que deben ser parseables como numero.
        label: "VISA" o "MASTERCARD", solo para el mensaje de error.
        mask: Filtro opcional de filas a considerar (ver _active_rule_mask) —
            usado para no rechazar el excel por basura preexistente en
            reglas YA VENCIDAS (caso real: 28 filas de Bahamas con
            rate_variable="1.75%", vigencia cerrada 2025-07/08 — ver
            gotchas.md. Sin este filtro, cualquier excel futuro heredaria
            ese mismo dato sucio historico y el refresh fallaria SIEMPRE,
            aunque el dato nunca se use).

    Returns:
        None.

    Raises:
        RulesValidationError: si alguna columna tiene valores no numericos
            en las filas consideradas (todas si mask es None).
    """
    problems = []
    for col in cols:
        if col not in df.columns:
            continue
        s = df[col] if mask is None else df[col][mask]
        non_null = s[s.notna() & (s.astype(str).str.strip() != "")]
        if non_null.empty:
            continue
        parsed = pd.to_numeric(non_null, errors="coerce")
        bad = non_null[parsed.isna()]
        if len(bad):
            examples = bad.unique()[:5].tolist()
            problems.append(f"{col} ({len(bad)} valores no numericos, ej: {examples})")
    if problems:
        raise RulesValidationError(
            f"{label}: columnas numericas con valores no parseables — {'; '.join(problems)}"
        )


def _active_rule_mask(df: pd.DataFrame, valid_until_col: str) -> pd.Series:
    """
    True para filas todavia vigentes (valid_until vacio/abierto, o >= hoy) —
    False para reglas ya vencidas. Usado para no aplicar guardas de calidad
    de dato sobre basura historica que ya no tiene efecto en el pipeline
    (ver _check_numeric_parseable).

    Args:
        df: DataFrame con la columna de vigencia (formato fecha string).
        valid_until_col: Nombre de la columna, ej. "VALID_UNTIL".

    Returns:
        Series booleana alineada al indice de df. Si la columna no existe,
        devuelve True para todas las filas (nada que filtrar).
    """
    if valid_until_col not in df.columns:
        return pd.Series(True, index=df.index)
    # tz-naive a proposito -- pd.to_datetime() sobre las fechas del excel
    # (strings sin timezone) tambien produce datetime64 tz-naive, y
    # comparar naive vs aware lanza TypeError en pandas.
    today = pd.Timestamp(datetime.utcnow().date())
    parsed = pd.to_datetime(df[valid_until_col], errors="coerce")
    return parsed.isna() | (parsed >= today)


def _build_visa_rules(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Replica read_rules_visa() de InterchangeRules.py (legacy), ya validada
    contra el excel real V37 en tst_files/interchange_rules/build_and_compare_rules.py,
    mas las validaciones duras nuevas (nulls/numericos, ver funciones de
    arriba).

    Args:
        raw_df: DataFrame leido directo del excel (dtype=str, sheet "Visa",
            skiprows=3).

    Returns:
        DataFrame con columnas en minuscula, listo para comparar/escribir
        como visa_rules/data.parquet.

    Raises:
        RulesValidationError: si falta alguna columna requerida, hay nulls
            en columnas requeridas, o algun valor numerico no parsea.

    Ejemplo:
        _build_visa_rules(raw_df)  # DataFrame, columnas en minuscula
    """
    df = raw_df.rename(columns={"DYNAMIC_CURRENCY_INDICATOR": "DYNAMIC_CURRENCY_CONVERSION_INDICATOR"})

    _check_required_columns(df, VISA_REQUIRED, "VISA")
    _check_required_nulls(df, VISA_REQUIRED, "VISA")
    _check_numeric_parseable(df, VISA_NUMERIC, "VISA")

    data = df.copy()
    for col in VISA_NUMERIC:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data.columns = data.columns.str.lower()
    if "transaction_code" in data.columns:
        data["transaction_code"] = data["transaction_code"].apply(_expand_transaction_code_family)
    return data


# =============================================================================
# VALIDACION Y CONSTRUCCION — MASTERCARD
# =============================================================================

def _build_mc_rules(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Replica read_rules_mc() de InterchangeRules.py (legacy), ya validada
    contra el excel real V23 en build_and_compare_rules.py, mas las
    validaciones duras nuevas. El renombrado final es posicional (mismo
    comportamiento que legacy) — por eso se exige que esten EXACTAMENTE las
    columnas de MC_SELECT_COLS, ni una de mas ni de menos.

    Args:
        raw_df: DataFrame leido directo del excel (dtype=str, sheet
            "Mastercard ITX & Service", skiprows=1).

    Returns:
        DataFrame con columnas en minuscula (MC_RENAME_FINAL), listo para
        comparar/escribir como mc_rules/data.parquet.

    Raises:
        RulesValidationError: si falta alguna columna esperada, el conteo de
            columnas no matchea exacto, hay nulls en columnas requeridas
            "duras", o algun valor numerico no parsea (en reglas vigentes
            para RATE_VARIABLE, en todas las filas para RATE_FIXED/MIN/CAP).

    Ejemplo:
        _build_mc_rules(raw_df)  # DataFrame, columnas en minuscula
    """
    missing = [c for c in MC_SELECT_COLS if c not in raw_df.columns]
    if missing:
        raise RulesValidationError(f"MC: columnas esperadas ausentes en el excel: {missing}")

    present = [c for c in MC_SELECT_COLS if c in raw_df.columns]
    if len(present) != len(MC_SELECT_COLS):
        raise RulesValidationError(
            "MC: el excel no trae exactamente las columnas esperadas por el proceso "
            f"({len(present)}/{len(MC_SELECT_COLS)}) — el renombrado es posicional, "
            "no se puede continuar sin revisar el excel."
        )

    data = raw_df[present].copy()

    _check_required_nulls(data, MC_REQUIRED_HARD, "MC")
    if any(data[c].isnull().any() for c in MC_REQUIRED_SOFT if c in data.columns):
        logger.info(
            "MC: nulls presentes en columnas tolerantes (CATEGORY/FEE_TIER) — "
            "se rellenan con sentinel, no es un error."
        )
    # RATE_FIXED/RATE_MIN/RATE_CAP se castean y almacenan -> chequeo estricto
    # sobre todas las filas. RATE_VARIABLE no se castea aca (legacy tambien
    # la deja como texto en esta etapa) -> solo se rechaza si el valor sucio
    # esta en una regla TODAVIA VIGENTE (ver _active_rule_mask/docstring de
    # _check_numeric_parseable) para no bloquear el refresh por basura
    # historica ya vencida que nunca se usa.
    _check_numeric_parseable(data, MC_NUMERIC, "MC")
    active_mask = _active_rule_mask(data, "VALID_UNTIL")
    _check_numeric_parseable(data, MC_NUMERIC_SOFT_CHECK, "MC (reglas vigentes)", mask=active_mask)

    for col in MC_NUMERIC:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data.columns = [c.lower() for c in MC_SELECT_COLS]

    # Mismo comportamiento que legacy pretende (no el bug real que tenia el
    # snapshot local de InterchangeRules.py sin fillna) — ver decisions.md /
    # memoria interchange_rules_excel_refresh.md, punto 7.
    cat = data["category"].fillna("").str.strip()
    pp = data["payment_product"]
    data["fee_category"] = cat.where(pp.isna(), cat + " " + pp.fillna(""))
    data = data[MC_RENAME_FINAL].copy()
    data["fee_tier"] = data["fee_tier"].fillna("S/I")
    return data


BUILDERS = {"VISA": _build_visa_rules, "MASTERCARD": _build_mc_rules}


# =============================================================================
# DIFF CONTRA EL PARQUET ACTUAL
# =============================================================================

def _normalize_value_cell(v) -> str:
    if pd.isna(v):
        return ""
    sv = str(v).strip()
    return "" if sv.lower() in NA_STRINGS else sv


def _normalize_cell_numeric_or_text(v) -> str:
    if pd.isna(v):
        return ""
    sv = str(v).strip()
    if sv.lower() in NA_STRINGS:
        return ""
    num = pd.to_numeric(sv, errors="coerce")
    if pd.notna(num):
        return f"{float(num):.6f}"
    return sv


def _normalize_for_diff(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza NaN/None/NaT -> "", fechas a YYYY-MM-DD, numeros a 6 decimales
    — para que la comparacion de valores no confunda diferencias reales con
    artefactos de formato/dtype entre el parquet leido de S3 y el excel
    reconstruido. Misma logica que build_and_compare_rules.py.

    Args:
        df: DataFrame a normalizar (no se modifica el original).

    Returns:
        Copia normalizada, todas las columnas como string.
    """
    out = df.copy()
    for col in out.columns:
        if col in DATE_LIKE_COLS:
            out[col] = out[col].apply(_normalize_value_cell).str.slice(0, 10)
        else:
            out[col] = out[col].apply(_normalize_cell_numeric_or_text)
    return out


def _compute_diff(new_df: pd.DataFrame, current_df: pd.DataFrame, key_cols: List[str]) -> Dict:
    """
    Compara el nuevo dataframe de reglas contra el parquet actual por llave
    de negocio (jurisdiction/guide_date/valid_from/intelica_id) — mismo
    criterio que compare() en build_and_compare_rules.py, pero devuelve un
    dict serializable en vez de imprimir/escribir CSV.

    Args:
        new_df: Reglas nuevas ya validadas (columnas en minuscula).
        current_df: Parquet actual leido de S3.
        key_cols: Columnas que forman la llave de comparacion.

    Returns:
        Dict con rows_current, rows_new, added, removed, common,
        rows_with_value_diff, columns_with_diff (conteo de filas afectadas
        por columna, solo las que tienen al menos 1 diferencia).
    """
    common_key = [k for k in key_cols if k in new_df.columns and k in current_df.columns]
    if not common_key:
        return {"warning": "sin llave comun para comparar filas"}

    new_n = _normalize_for_diff(new_df)
    cur_n = _normalize_for_diff(current_df)

    new_idx = new_n.set_index(common_key)
    cur_idx = cur_n.set_index(common_key)

    added = new_idx.index.difference(cur_idx.index)
    removed = cur_idx.index.difference(new_idx.index)
    common = new_idx.index.intersection(cur_idx.index)

    common_val_cols = [c for c in new_idx.columns if c in cur_idx.columns]
    new_c = new_idx.loc[common, common_val_cols]
    cur_c = cur_idx.loc[common, common_val_cols]
    new_c = new_c[~new_c.index.duplicated(keep="first")]
    cur_c = cur_c[~cur_c.index.duplicated(keep="first")]
    common2 = new_c.index.intersection(cur_c.index)
    new_c, cur_c = new_c.loc[common2], cur_c.loc[common2]

    diff_mask = new_c != cur_c
    rows_with_diff = int(diff_mask.any(axis=1).sum())
    col_diffs = diff_mask.sum().sort_values(ascending=False)
    col_diffs = {str(k): int(v) for k, v in col_diffs[col_diffs > 0].items()}

    return {
        "rows_current": int(len(current_df)),
        "rows_new": int(len(new_df)),
        "added": int(len(added)),
        "removed": int(len(removed)),
        "common": int(len(common2)),
        "rows_with_value_diff": rows_with_diff,
        "columns_with_diff": col_diffs,
    }


def _check_row_ratio(new_len: int, current_len: int, label: str) -> None:
    """
    Falla dura si el excel nuevo trae muchas menos filas que el parquet
    actual — guarda barata contra "subieron el sheet equivocado" o un
    archivo truncado. No aplica en la primera carga (current_len == 0).

    Args:
        new_len: Filas del excel nuevo ya validado.
        current_len: Filas del parquet actual en S3.
        label: "VISA" o "MASTERCARD", solo para el mensaje de error.

    Returns:
        None.

    Raises:
        RulesValidationError: si new_len/current_len < MIN_ROW_RATIO.
    """
    if current_len == 0:
        return
    ratio = new_len / current_len
    if ratio < MIN_ROW_RATIO:
        raise RulesValidationError(
            f"{label}: el excel nuevo trae {new_len:,} filas vs {current_len:,} actuales "
            f"({ratio:.0%}) — por debajo del umbral minimo ({MIN_ROW_RATIO:.0%}), "
            "posible archivo incorrecto o truncado."
        )


# =============================================================================
# HELPERS S3
# =============================================================================

def _object_exists(bucket: str, key: str) -> bool:
    """
    Args:
        bucket: Bucket S3.
        key: Key a chequear.

    Returns:
        True si el objeto existe, False si da 404/NoSuchKey.

    Raises:
        ClientError: cualquier otro error (permisos, etc.) se relanza.
    """
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code in ('404', 'NoSuchKey', 'NotFound'):
            return False
        raise


def _download_bytes(bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)['Body'].read()


def _read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    return pd.read_parquet(BytesIO(_download_bytes(bucket, key)))


def _write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def _copy_object(bucket: str, src_key: str, dest_key: str) -> None:
    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': src_key}, Key=dest_key)


def _move_object(bucket: str, src_key: str, dest_key: str) -> None:
    _copy_object(bucket, src_key, dest_key)
    s3.delete_object(Bucket=bucket, Key=src_key)


# =============================================================================
# PROCESAMIENTO DE UN ARCHIVO
# =============================================================================

def _process_record(bucket: str, key: str) -> Dict:
    """
    Procesa un excel de reglas subido: valida, calcula diff, respalda el
    parquet actual, publica el nuevo, archiva el excel fuente y registra el
    resultado. Ver docstring del modulo para el detalle de cada paso.

    Args:
        bucket: Bucket S3 (s3-reference).
        key: Key del excel subido bajo interchange_rules/{marca}/.

    Returns:
        Dict con el resultado: status en {"SUCCESS", "FAILED", "SKIPPED"},
        mas detalles segun el caso.
    """
    if _is_admin_subpath(key):
        return {'file': key, 'status': 'SKIPPED', 'reason': 'admin subpath'}

    brand = _detect_brand(key)
    if brand is None:
        logger.warning(f"Key fuera de scope, sin marca reconocida: {key}")
        return {'file': key, 'status': 'SKIPPED', 'reason': 'unrecognized brand/prefix'}

    filename = key.split('/')[-1]
    if not filename or filename.startswith('.') or not filename.lower().endswith(('.xlsx', '.xls')):
        return {'file': key, 'status': 'SKIPPED', 'reason': 'not an excel file'}

    cfg = BRAND_CONFIG[brand]
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    logger.info(f"=== Procesando reglas {brand}: s3://{bucket}/{key} ===")

    try:
        raw_bytes = _download_bytes(bucket, key)
        try:
            raw_df = pd.read_excel(
                BytesIO(raw_bytes), sheet_name=cfg['sheet_name'],
                dtype=str, skiprows=cfg['skiprows'], engine='openpyxl',
            )
        except Exception as e:
            # Poison pill, igual que RulesValidationError (ver docstring
            # del modulo) -- reintentar no arregla una hoja/formato invalido.
            raise RulesValidationError(
                f"{brand}: no se pudo leer el excel (se esperaba la hoja "
                f"'{cfg['sheet_name']}') — {e}"
            ) from e
        logger.info(f"  Excel leido: {raw_df.shape[0]:,} filas, {raw_df.shape[1]} columnas")

        new_df = BUILDERS[brand](raw_df)

        current_exists = _object_exists(bucket, cfg['rules_key'])
        current_df = _read_parquet_from_s3(bucket, cfg['rules_key']) if current_exists else None

        if current_df is not None:
            _check_row_ratio(len(new_df), len(current_df), brand)

    except RulesValidationError as e:
        logger.warning(f"  Validacion fallida: {e}")
        rejected_key = f"{cfg['incoming_prefix']}_rejected/{ts}_{filename}"
        try:
            _move_object(bucket, key, rejected_key)
            logger.info(f"  Excel rechazado movido a: s3://{bucket}/{rejected_key}")
        except Exception as move_err:
            logger.error(f"  No se pudo mover el excel rechazado: {move_err}")
        return {'file': filename, 'status': 'FAILED', 'brand': brand, 'error': str(e)}

    # A partir de aca solo quedan errores de infraestructura (S3) — se dejan
    # propagar para que el retry automatico de Lambda async actue, y el
    # excel fuente NO se mueve (queda disponible para diagnostico/retry).
    diff_summary = _compute_diff(new_df, current_df, cfg['key_cols']) if current_df is not None else None

    backup_key = None
    if current_exists:
        backup_key = f"{cfg['history_prefix']}data_{ts}.parquet"
        _copy_object(bucket, cfg['rules_key'], backup_key)
        logger.info(f"  Backup del parquet actual: s3://{bucket}/{backup_key}")

    _write_parquet_to_s3(new_df, bucket, cfg['rules_key'])
    logger.info(f"  Publicado: s3://{bucket}/{cfg['rules_key']} ({len(new_df):,} filas)")

    archived_key = f"{cfg['incoming_prefix']}_archive/{ts}_{filename}"
    _move_object(bucket, key, archived_key)
    logger.info(f"  Excel archivado en: s3://{bucket}/{archived_key}")

    logger.info(f"  Diff vs actual: {json.dumps(diff_summary)}")

    return {
        'file': filename, 'status': 'SUCCESS', 'brand': brand,
        'rows_new': len(new_df), 'backup_s3_key': backup_key, 'diff_summary': diff_summary,
    }


# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Punto de entrada. Se dispara por eventos S3 ObjectCreated en
    s3-reference, prefijo interchange_rules/{VISA|MASTERCARD}/. Un evento
    puede traer varios records (batch S3) — cada uno se procesa de forma
    independiente, un fallo en uno no aborta los demas.

    Args:
        event: Evento S3 con event['Records'].
        context: Contexto de ejecucion de Lambda (no usado).

    Returns:
        Dict con statusCode=200 y body (JSON) con la lista de resultados.
    """
    logger.info("=== ITX Rules Refresh Lambda ===")
    _validar_configuracion()

    results = []
    for record in event.get('Records', []):
        try:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])
            results.append(_process_record(bucket, key))
        except Exception as e:
            logger.error(f"Error inesperado procesando record: {e}", exc_info=True)
            raise  # error de infraestructura -> dejar que Lambda reintente

    logger.info(f"Resultados: {json.dumps(results, default=str)}")
    return {'statusCode': 200, 'body': json.dumps(results, default=str)}
