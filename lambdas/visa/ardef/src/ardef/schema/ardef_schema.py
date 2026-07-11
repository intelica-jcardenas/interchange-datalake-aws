"""
ardef_schema.py

Definición estática (hardcodeada, no vive en DynamoDB a diferencia de los
campos Visa/Mastercard transaccionales) del layout de campos ARDEF de Visa:
posiciones de ancho fijo de cada campo dentro de la línea de texto plano del
archivo fuente, más los metadatos de control que el pipeline agrega a cada
registro. Usado por las etapas de transform/extract del motor de reglas
ARDEF para parsear el archivo ARDEF y para tipar las columnas del parquet
resultante.
"""

METADATA_SCHEMA: dict[str, dict[str, str]] = {
    "file_id":                  {"data_type":'text'},
    "file_processing_date":     {"data_type":'text'},
    "ardef_version":            {"data_type":'text'},
    "ardef_header_date":        {"data_type":'text'},
    "line_no":                  {"data_type":'integer'},
    "lines":                    {"data_type":'text'},
    "row_creation_timestamp":   {"data_type":'text'},
    "_eff_ts":                  {"data_type":'text'},
}

# Cada campo contiene:
#   start       -> posicion inicial (inclusive) en la linea de texto
#   end         -> posicion final (exclusive) en la linea de texto
#   data_type   -> tipo de dato de referencia
#
#
ARDEF_SCHEMA: dict[str, dict[str, int]] = { 
    "table_type":                                           {"start": 0,    "end": 2,       "data_type": 'text' },
    "table_mnemonic":                                       {"start": 2,    "end": 10,      "data_type": 'text' },
    "record_type":                                          {"start": 10,   "end": 11,      "data_type": 'text' },
    "table_key":                                            {"start": 11,   "end": 23,      "data_type": 'integer' }, # data_type
    "effective_date":                                       {"start": 23,   "end": 31,      "data_type": 'text' },
    "delete_indicator":                                     {"start": 31,   "end": 32,      "data_type": 'text' },
    "low_key_for_range":                                    {"start": 32,   "end": 44,      "data_type": 'integer' }, # data_type
    "issuer_identifier":                                    {"start": 44,   "end": 50,      "data_type": 'text' },
    "check_digit_algorithm":                                {"start": 50,   "end": 51,      "data_type": 'text' },
    "account_number_length":                                {"start": 51,   "end": 53,      "data_type": 'text' },
    "token_indicator":                                      {"start": 53,   "end": 54,      "data_type": 'text' },
    "clearing_only_indicator":                              {"start": 54,   "end": 55,      "data_type": 'text' }, # replace reserved
    "base_ii_cib":                                          {"start": 55,   "end": 61,      "data_type": 'text' },
    "domain":                                               {"start": 61,   "end": 62,      "data_type": 'text' },
    "region":                                               {"start": 62,   "end": 63,      "data_type": 'text' },
    "country":                                              {"start": 63,   "end": 65,      "data_type": 'text' },
    "large_ticket":                                         {"start": 65,   "end": 66,      "data_type": 'text' },
    "technology_indicator":                                 {"start": 66,   "end": 67,      "data_type": 'text' },
    "ardef_region":                                         {"start": 67,   "end": 68,      "data_type": 'text' },
    "ardef_country":                                        {"start": 68,   "end": 70,      "data_type": 'text' },
    "commercial_card_level_2_data_indicator":               {"start": 70,   "end": 71,      "data_type": 'text' },
    "commercial_card_level_3_enhanced_data_indicator":      {"start": 71,   "end": 72,      "data_type": 'text' },
    "commercial_card_pos_prompting_indicator":              {"start": 72,   "end": 73,      "data_type": 'text' },
    "commercial_card_electronic_vat_evidence_indicator":    {"start": 73,   "end": 74,      "data_type": 'text' },
    "original_credit":                                      {"start": 74,   "end": 75,      "data_type": 'text' },
    "account_level_processing_indicator":                   {"start": 75,   "end": 76,      "data_type": 'text' },
    "original_credit_money_transfer":                       {"start": 76,   "end": 77,      "data_type": 'text' },
    "original_credit_online_gambling":                      {"start": 77,   "end": 78,      "data_type": 'text' },
    "product_id":                                           {"start": 78,   "end": 80,      "data_type": 'text' },
    "combo_card":                                           {"start": 80,   "end": 81,      "data_type": 'text' },
    "fast_funds":                                           {"start": 81,   "end": 82,      "data_type": 'text' },
    "travel_indicator":                                     {"start": 82,   "end": 83,      "data_type": 'text' },
    "b2b_program_id":                                       {"start": 83,   "end": 85,      "data_type": 'text' },
    "program_indicator":                                    {"start": 85,   "end": 86,      "data_type": 'text' }, # Rename prepaid_program_indicator
    "rr12":                                                 {"start": 86,   "end": 88,      "data_type": 'text' }, # Rename rrr13 | CHANGE LENGTH: {"start": 86, "end": 89}
    "multi_account_access_indicator":                       {"start": 88,   "end": 89,      "data_type": 'text' }, # new field
    "account_funding_source":                               {"start": 89,   "end": 90,      "data_type": 'text' }, 
    "settlement_match":                                     {"start": 90,   "end": 91,      "data_type": 'text' }, 
    "travel_account_data":                                  {"start": 91,   "end": 92,      "data_type": 'text' }, 
    "account_restricted_use":                               {"start": 92,   "end": 93,      "data_type": 'text' }, 
    "nnss_indicator":                                       {"start": 93,   "end": 94,      "data_type": 'text' }, 
    "product_subtype":                                      {"start": 94,   "end": 96,      "data_type": 'text' }, 
    "alternate_atm":                                        {"start": 96,   "end": 97,      "data_type": 'text' }, 
    "reserved_1":                                           {"start": 97,   "end": 98,      "data_type": 'text' }, 
    "reserved_2":                                           {"start": 98,   "end": 100,     "data_type": 'text' }, 
}