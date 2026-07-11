"""
transform.py

Capa TRANSFORM del pipeline IAR (Mastercard) — módulo interno importado por
`handler.py` (`pipeline_iar`, paso "TRANSFORM"). Define el layout de
posiciones fijas de cada tabla IPM soportada (hoy solo IP0040T1, rangos de
BIN/reglas) y expande los records de detalle de la capa RAW en columnas de
negocio, análogo a la etapa `extract` de Visa/Mastercard en el flujo
transaccional pero para archivos de reglas.
"""


from logs.logger import logger

import pandas as pd

def getIPMParameters()->dict:
        """
        Layout de posiciones fijas (start/end en el texto del record) para
        interpretar los distintos tipos de record de un archivo IAR: los dos
        formatos de header ("update_header"/"replace_header", ver
        `parse_header_raw` en `raw.py`), el catálogo IP0000T1 ("key") y las
        columnas de cada tabla de negocio soportada bajo "tables" (hoy solo
        "IP0040T1"). `low_range`/`gcms_product` están marcados como parte de
        la llave de negocio (comentario `# part_of_key`, ver `BUSINESS_KEYS`
        en `calculate.py`).

        Returns:
           dict con la estructura de layouts descrita arriba, usado por
           `transform_iar_table_from_raw` para cortar cada record.

        Ejemplo:
            params = getIPMParameters()
            params["tables"]["IP0040T1"]["low_range"]  # {"start": 11, "end": 30, "data_type": "int64"}
        """

        params = {
            "update_header": {
                "header": {
                    "header_title": {"start": 0, "end": 15},
                    "header_date": {"start": 15, "end": 23},
                    "header_time": {"start": 23, "end": 28},
                }
            },
            "replace_header": {
                "header": {
                    "header_title": {"start": 0, "end": 17},
                    "header_date": {"start": 45, "end": 54},
                    "header_time": {"start": 61, "end": 69},
                }
            },
            "key": {
                "layout": "IP0000T1",
                "key": {"start": 11, "end": 19},
                "table_ipm_id": {"start": 19, "end": 27},
                "table_sub_id": {"start": 243, "end": 246},
            },
            "record": {"start": 8, "end": 11},
            "tables": {
                "IP0040T1": {
                    "effective_timestamp": {"start": 0, "end": 7},
                    "active_inactive_code": {"start": 7, "end": 8},
                    "table_id": {"start": 8, "end": 11},
                    "low_range": {"start": 11, "end": 30,"data_type":"int64"},  # part_of_key
                    "gcms_product": {"start": 30, "end": 33},  # part_of_key
                    "high_range": {"start": 33, "end": 52,"data_type":"int64"},
                    "card_program_identifier": {"start": 52, "end": 55},
                    "card_program_priority": {"start": 55, "end": 57},
                    "member_id": {"start": 57, "end": 68},
                    "product_type": {"start": 68, "end": 69},
                    "endpoint": {"start": 69, "end": 76},
                    "card_country_alpha": {"start": 76, "end": 79},
                    "card_country_numeric": {"start": 79, "end": 82},
                    "region": {"start": 82, "end": 83},
                    "product_class": {"start": 83, "end": 86},
                    "tran_routing_ind": {"start": 86, "end": 87},
                    "first_present_reassign_ind": {"start": 87, "end": 88},
                    "product_reassign_switch": {"start": 88, "end": 89},
                    "pwcb_optin_switch": {"start": 89, "end": 90},
                    "licensed_product_id": {"start": 90, "end": 93},
                    "mapping_service_ind": {"start": 93, "end": 94},
                    "alm_participation_ind": {"start": 94, "end": 95},
                    "alm_activation_date": {"start": 95, "end": 101},
                    "cardholder_billing_currency_default": {"start": 101, "end": 104},
                    "cardholder_billing_currency_exponent_default": {
                        "start": 104,
                        "end": 105,
                    },
                    "cardholder_billing_primary_currency": {"start": 105, "end": 133},
                    "chip_to_magnetic": {"start": 133, "end": 134},
                    "floor_expiration_date": {"start": 134, "end": 140},
                    "co_brand_participation_switch": {"start": 140, "end": 141},
                    "spend_control_switch": {"start": 141, "end": 142},
                    "merchant_cleansing_service": {"start": 142, "end": 145},
                    "merchant_cleansing_activation": {"start": 145, "end": 151},
                    "contactless_enabled_indicator": {"start": 151, "end": 152},
                    "regulated_rate_type": {"start": 152, "end": 153},
                    "psn_route_indicator": {"start": 153, "end": 154},
                    "cashback_without_purchase_indicator": {"start": 154, "end": 155},
                    # "filler_1":{"start":155,"end":156},
                    "repower_reload_participation_indicator": {
                        "start": 156,
                        "end": 157,
                    },
                    "moneysend_indicator": {"start": 157, "end": 158},
                    "durbin_regulated_rate_indicator": {"start": 158, "end": 159},
                    "cash_access_only_participating_indicator": {
                        "start": 159,
                        "end": 160,
                    },
                    "authenticator_indicator": {"start": 160, "end": 161},
                    # "filler_2":{"start":161,"end":162},
                    "issuer_target_market_participation_indicator": {
                        "start": 162,
                        "end": 163,
                    },
                    "post_date_service_indicator": {"start": 163, "end": 164},
                    "meal_voucher_indicator": {"start": 164, "end": 165},
                    "non_reloadable_prepaid_switch": {"start": 165, "end": 167},
                    "faster_funds_indicator": {"start": 167, "end": 168},
                    "anonymous_prepaid_indicator": {"start": 168, "end": 169},
                    "cardholder_currency_indicator": {"start": 169, "end": 170},
                    "pay_by_account_indicator": {"start": 170, "end": 171},
                    "issuer_account_range_gaming_participation_indicator":{"start":171,"end":172},
                }
            },
        }

        return params

def transform_iar_table_from_raw(
    df_records: pd.DataFrame,
    df_catalog: pd.DataFrame,
    df_header: pd.DataFrame,
    table_to_look: str,
    params: dict,
    client_id,
    file_id,
) -> pd.DataFrame:
    """
    Expande los records de detalle de una tabla de negocio (ej. IP0040T1) en
    columnas, usando el layout de posiciones fijas de `params["tables"]` y el
    `table_sub_id` correspondiente (obtenido del catálogo IP0000T1). Agrega
    también metadatos de trazabilidad (`app_full_data`, `app_processing_date`,
    `app_type_file`, `app_customer_code`, `app_hash_file`, etc.) a cada fila.

    Args:
        df_records: DataFrame de records de detalle (capa RAW, todas las
            tablas mezcladas).
        df_catalog: DataFrame del catálogo IP0000T1 (capa RAW), usado para
            resolver `table_sub_id` a partir de `table_to_look`.
        df_header: DataFrame del header (capa RAW), 1 fila.
        table_to_look: Nombre de la tabla de negocio a extraer, ej.
            "IP0040T1".
        params: dict de layouts, salida de `getIPMParameters()`.
        client_id: Identificador del cliente (se agrega como
            `app_customer_code`).
        file_id: Identificador del archivo (se agrega como `app_hash_file`).

    Returns:
        DataFrame con una fila por record de detalle de `table_to_look`, con
        sus columnas de negocio ya cortadas por posición más los metadatos
        de trazabilidad.

    Raises:
        ValueError: si `table_to_look` no existe en `df_catalog`.

    Ejemplo:
        transform_iar_table_from_raw(df_records, df_catalog, df_header,
            "IP0040T1", getIPMParameters(), "SBSA", "ABC123...")
    """

    catalog_match = df_catalog[df_catalog["table_ipm_id"] == table_to_look]

    if catalog_match.empty:
        raise ValueError(f"La tabla {table_to_look} no existe en el catálogo")

    table_sub_id = catalog_match.iloc[0]["table_sub_id"]

    header_type = df_header.iloc[0]["header_type"]
    processing_date = df_header.iloc[0]["app_processing_date"]
    

    df_table_records = df_records[
        (df_records["record_type"] == "DETAIL")
        & (df_records["record_table_id"] == table_sub_id)
    ]   

    table_params = params["tables"][table_to_look]
    rows = []

    for _, record in df_table_records.iterrows():
        record_raw = record["record_raw"]

        row = {}

        for field_name, cfg in table_params.items():
            row[field_name] = record_raw[cfg["start"]:cfg["end"]]

        row["app_full_data"] = record_raw
        row["app_processing_date"] = processing_date #record["app_processing_date"]
        row["app_type_file"] = "IAR"
        row["app_customer_code"] = client_id
        row["app_hash_file"] = file_id
        row["app_header_type"] = header_type
        row["source_file"] = record["source_file"]
        row["record_sequence"] = record["record_sequence"]
        row["record_table_id"] = record["record_table_id"]
        row["table_ipm_id"] = table_to_look
    

        rows.append(row)

    return pd.DataFrame(rows)