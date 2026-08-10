from __future__ import annotations
import argparse
import csv
import io
import sys
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, ROUND_DOWN
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    AWS_GLUE_AVAILABLE = True
except ImportError:
    GlueContext = None
    Job = None
    getResolvedOptions = None
    SparkContext = None
    AWS_GLUE_AVAILABLE = False

SHARED_MASTER_ROOT = 's3://itl-0004-itx-dev-intchg-02-s3-reference'
REPORT_MASTER_ROOT = 's3://itl-0004-itx-dev-intchg-02-s3-reference/reports/ebgr_reports'

def is_s3_uri(path: Any) -> bool:
    return str(path).lower().startswith('s3://')

def is_uri(path: Any) -> bool:
    s = str(path).lower()
    return s.startswith('s3://') or s.startswith('s3a://') or s.startswith('s3n://')

def join_path(root: Any, *parts: str) -> str:
    root_s = str(root).rstrip('/')
    clean_parts = [str(p).strip('/') for p in parts if str(p) not in ('', '.')]
    if is_uri(root_s):
        return '/'.join([root_s] + clean_parts) if clean_parts else root_s
    return str(Path(root_s, *clean_parts))

def ensure_parent_dir(path: Any) -> None:
    if is_uri(path):
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def ensure_dir(path: Any) -> None:
    if is_uri(path):
        return
    Path(path).mkdir(parents=True, exist_ok=True)

def path_exists(spark, path: Any) -> bool:
    path_s = str(path)
    if not is_uri(path_s):
        return Path(path_s).exists()
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI.create(path_s)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path_s))

def split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != 's3' or not parsed.netloc:
        raise ValueError(f'Not an s3:// URI: {uri}')
    return (parsed.netloc, parsed.path.lstrip('/'))

def has_col(df: DataFrame, name: str) -> bool:
    return name in df.columns

def c(df: DataFrame, name: str):
    if has_col(df, name):
        return F.col(name)
    return F.lit(None)

def first_existing_col(df: DataFrame, names: Iterable[str], default=None):
    exprs = [F.col(name) for name in names if has_col(df, name)]
    if not exprs:
        return F.lit(default)
    return F.coalesce(*exprs)

def to_int(expr):
    return expr.cast(T.IntegerType())

def to_long(expr):
    return expr.cast(T.LongType())

def to_decimal(expr, precision: int=28, scale: int=6):
    return F.regexp_replace(expr.cast('string'), ',', '').cast(T.DecimalType(precision, scale))

def clean_string(expr):
    return F.trim(expr.cast('string'))

def read_master_dataset(spark: SparkSession, master_root: Any, dataset_name: str) -> DataFrame:
    folder_path = join_path(master_root, dataset_name)
    file_path = join_path(master_root, f'{dataset_name}.parquet')
    if path_exists(spark, folder_path):
        return spark.read.parquet(folder_path)
    if path_exists(spark, file_path):
        return spark.read.parquet(file_path)
    raise FileNotFoundError(f'Master dataset not found. Tried: {folder_path} and {file_path}')

def read_parquet_dataset(spark: SparkSession, raw_root: Any, relative_path: str) -> DataFrame:
    return spark.read.option('basePath', str(raw_root)).parquet(join_path(raw_root, relative_path))

def union_by_name_allow_missing(dfs: list[DataFrame]) -> DataFrame:
    if not dfs:
        raise ValueError('No DataFrames to union')
    out = dfs[0]
    for df in dfs[1:]:
        out = out.unionByName(df, allowMissingColumns=True)
    return out

def filter_by_date(df: DataFrame, begin_date: str, end_date: str, date_col: str='date') -> DataFrame:
    if date_col in df.columns:
        return df.where((F.to_date(F.col(date_col)) >= F.lit(begin_date)) & (F.to_date(F.col(date_col)) <= F.lit(end_date)))
    return df
_EEA_ALPHA2 = [
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
    "FO", "FR", "GB", "GF", "GI", "GP", "GR", "HR", "HU", "IE",
    "IS", "IT", "LI", "LT", "LU", "LV", "MF", "MQ", "MT", "NL",
    "NO", "PL", "PT", "RE", "RO", "SE", "SI", "SK", "YT"
]

def country_master_select(country_df: DataFrame, prefix: str) -> DataFrame:
    cols = country_df.columns
    country_numeric = F.col('country_numeric') if 'country_numeric' in cols else F.lit(None)
    country_code = F.col('country_code') if 'country_code' in cols else F.lit(None)
    country_code_alt = F.col('country_code_alternative') if 'country_code_alternative' in cols else F.lit(None)
    legacy_country_id = F.col('legacy_country_id') if 'legacy_country_id' in cols else F.lit(None)
    mc_region = F.col('mastercard_region_code') if 'mastercard_region_code' in cols else F.lit(None)
    alpha2 = F.upper(clean_string(country_code))
    eea = F.when(alpha2.isin(_EEA_ALPHA2), F.lit(1)).otherwise(F.lit(0))
    return country_df.select(to_int(legacy_country_id).alias(f'{prefix}_CTRY_ID'), clean_string(country_numeric).alias(f'{prefix}_CTRY_NUM'), F.upper(clean_string(country_code)).alias(f'{prefix}_CTRY_AN2'), F.upper(clean_string(country_code_alt)).alias(f'{prefix}_CTRY_AN3'), clean_string(country_code).alias(f'{prefix}_COUNTRY_CODE'), clean_string(mc_region).alias(f'{prefix}_MC_REGION_CODE'), eea.cast('int').alias(f'{prefix}_EEA_FLG_ID')).dropDuplicates([f'{prefix}_CTRY_ID', f'{prefix}_CTRY_NUM', f'{prefix}_CTRY_AN2', f'{prefix}_CTRY_AN3'])

def country_lookup_join_master(df: DataFrame, country_df: DataFrame, raw_country_expr, output_col: str, prefix: str) -> DataFrame:
    lk = country_master_select(country_df, prefix)
    tmp = df.withColumn(f'{prefix}_raw_country', clean_string(raw_country_expr))
    cond = (clean_string(F.col(f'{prefix}_raw_country')) == clean_string(F.col(f'{prefix}_CTRY_ID'))) | (clean_string(F.col(f'{prefix}_raw_country')) == clean_string(F.col(f'{prefix}_CTRY_NUM'))) | (F.upper(clean_string(F.col(f'{prefix}_raw_country'))) == F.upper(clean_string(F.col(f'{prefix}_CTRY_AN2')))) | (F.upper(clean_string(F.col(f'{prefix}_raw_country'))) == F.upper(clean_string(F.col(f'{prefix}_CTRY_AN3'))))
    tmp = tmp.join(F.broadcast(lk), cond, 'left')
    return tmp.withColumn(output_col, to_int(F.col(f'{prefix}_CTRY_ID')))

TXN_SCHEME_FEE_COLUMNS = ['ID_VI_MC', 'ID_MC', 'SET_DT', 'MCT_CD', 'ARG_FND_SRC_ID', 'ARG_CTRY_ID', 'ARG_PRD_ID', 'TRN_SCP_ID', 'TRN_TYP_ID', 'TRN_CYC_ID', 'TRN_RVSL_FLG_ID', 'CRNCY_LCL_IND', 'CRD_PRS_IND', 'ACS_FEE_IND', 'TRN_CNT', 'SET_AMT', 'IRF_AMT', 'SUB_SCH_ID', 'SCH_ID']

def _mastercard_scheme_product(spark: SparkSession, master_root: Any) -> DataFrame:
    raw = read_master_dataset(spark, master_root, 'scheme_fee_bin_products')
    brand_expr = F.upper(clean_string(F.col('brand'))) if 'brand' in raw.columns else F.lit('MASTERCARD')
    return raw.where(brand_expr.rlike('MC|MASTER') | brand_expr.isNull()).select(F.upper(clean_string(F.col('product_code'))).alias('lk_sch_prd_cd'), to_int(F.col('legacy_product_id')).alias('lk_sch_prd_id'), to_int(F.col('rng_fnd_id')).alias('lk_rng_fnd_id'), to_int(F.col('range_program_id')).alias('lk_rng_prg_id'), F.lit(1).cast('int').alias('lk_sch_id')).where(F.col('lk_sch_prd_cd').isNotNull()).dropDuplicates(['lk_sch_prd_cd'])

def _mastercard_txn_type(spark: SparkSession, master_root: Any) -> DataFrame:
    raw = read_master_dataset(spark, master_root, 'mastercard_business_transaction_type')
    tx_label = F.upper(clean_string(F.col('transaction_type_id')))
    trx_typ_id = F.when(tx_label == F.lit('PUR'), F.lit(1)).when(tx_label == F.lit('CSH'), F.lit(8)).otherwise(to_int(F.col('transaction_type_id')))
    return raw.select(F.lpad(clean_string(F.col('business_transaction_type_id')), 2, '0').alias('lk_txn_typ_cd'), trx_typ_id.cast('int').alias('lk_trx_typ_id'), tx_label.alias('lk_txn_typ_label')).where(F.col('lk_txn_typ_cd').isNotNull()).dropDuplicates(['lk_txn_typ_cd'])

def build_mastercard_txn_scheme_fee(spark: SparkSession, raw_root: Any, master_root: Any, begin_date: str, end_date: str) -> DataFrame:
    dfs: list[DataFrame] = []
    for dataset in ['MC/IPM_1240', 'MC/IPM_1442']:
        dataset_path = join_path(raw_root, dataset)
        if path_exists(spark, dataset_path):
            dfs.append(filter_by_date(read_parquet_dataset(spark, raw_root, dataset), begin_date, end_date))
    if not dfs:
        raise FileNotFoundError('No Mastercard raw Parquet datasets found for MC/IPM_1240 or MC/IPM_1442')
    mc = union_by_name_allow_missing(dfs)
    scheme_product = _mastercard_scheme_product(spark, master_root)
    mc_txn_type = _mastercard_txn_type(spark, master_root)
    country = read_master_dataset(spark, master_root, 'country')
    df = mc
    product_code = first_existing_col(df, ['gcms_product_identifier', 'licensed_product_identifier_pds_3', 'gcms_product_identifier_pds_2', 'card_program_identifier'])
    df = df.join(F.broadcast(scheme_product), F.upper(clean_string(product_code)) == F.col('lk_sch_prd_cd'), 'left').join(F.broadcast(mc_txn_type), F.lpad(clean_string(first_existing_col(df, ['cardholder_transaction_type_de_3_1'])), 2, '0') == F.col('lk_txn_typ_cd'), 'left')
    raw_country = first_existing_col(df, ['jurisdiction_country', 'iar_country', 'region_country_code', 'card_acceptor_country_code_de_43_6'])
    df = country_lookup_join_master(df, country, raw_country, 'ARG_CTRY_ID', 'mc_country')
    pds165_01 = clean_string(first_existing_col(df, ['settlement_indicator_1_pds_165_1']))
    excluded = to_int(first_existing_col(df, ['exclude_flag'], '0'))
    biz_svc = to_int(first_existing_col(df, ['biz_svc_typ_id', 'business_service_type_id', 'business_service_id', 'business_service_type', 'biz_service_type_id']))
    has_numeric_biz_col = any((name in df.columns for name in ['biz_svc_typ_id', 'business_service_type_id', 'business_service_id', 'business_service_type', 'biz_service_type_id']))
    biz_filter = biz_svc.isNotNull() if has_numeric_biz_col else F.lit(True)
    df = df.where(pds165_01.isin('C', 'M') & (F.coalesce(excluded, F.lit(0)) == 0) & clean_string(product_code).isNotNull() & (F.upper(clean_string(product_code)) != F.lit('GCP')) & biz_filter)
    jurisdiction_raw = F.lower(clean_string(first_existing_col(df, ['jurisdiction'], '')))
    country_scope = F.when(F.col('ARG_CTRY_ID') == 90, F.lit(2))
    if 'mc_country_EEA_FLG_ID' in df.columns:
        country_scope = country_scope.when(to_int(F.col('mc_country_EEA_FLG_ID')) == 1, F.lit(4))
    country_scope = country_scope.otherwise(F.lit(8))
    biz_scope = F.when(biz_svc.isin(4) & (pds165_01 == 'C'), F.lit(1)).when(biz_svc.isin(4) & (pds165_01 == 'M'), F.lit(2)).when(biz_svc.isin(2, 3), F.lit(4)).when(biz_svc.isin(1, 8), F.lit(8))
    jurisdiction_scope = F.when(jurisdiction_raw.rlike('inter'), F.lit(8)).when(jurisdiction_raw.rlike('intra'), F.lit(4)).when(jurisdiction_raw.rlike('off-us|domestic|local|intra.?country'), F.lit(2))
    trn_scp_id = F.coalesce(biz_scope, jurisdiction_scope, country_scope)
    trx_currency = clean_string(first_existing_col(df, ['currency_code_transaction_de_49', 'currency_code_reconciliation_de_50']))
    report_currency = clean_string(first_existing_col(df, ['settlement_report_currency_id', 'settlement_report_currency_code', 'currency_code_reconciliation_de_50']))
    crncy_lcl_ind = F.when(trx_currency == report_currency, F.lit(1)).otherwise(F.lit(0))
    card_present = clean_string(first_existing_col(df, ['card_present_data_de_22_6']))
    reversal_raw = clean_string(first_existing_col(df, ['message_reversal_indicator_pds_25']))
    set_amt = to_decimal(first_existing_col(df, ['settlement_report_amount', 'amount_reconciliation_de_5', 'amount_transaction'], '0'))
    irf_amt = to_decimal(first_existing_col(df, ['amounts_transaction_fee_7_pds_146_7', 'calculated_value'], '0'))
    card_program = clean_string(first_existing_col(df, ['card_program_identifier', 'licensed_product_identifier_pds_3']))
    out = df.select(clean_string(first_existing_col(df, ['file_idn'])).alias('ID_VI_MC'), clean_string(first_existing_col(df, ['ref_id'])).alias('ID_MC'), F.to_date(first_existing_col(df, ['date', 'file_dt', 'file_processing_date'])).alias('SET_DT'), F.lpad(clean_string(first_existing_col(df, ['card_acceptor_id_code_de_42'])), 11, '0').alias('MCT_CD'), F.col('lk_rng_fnd_id').cast('int').alias('ARG_FND_SRC_ID'), F.col('ARG_CTRY_ID').cast('int').alias('ARG_CTRY_ID'), F.col('lk_sch_prd_id').cast('string').alias('ARG_PRD_ID'), trn_scp_id.cast('int').alias('TRN_SCP_ID'), F.when(F.lpad(clean_string(first_existing_col(df, ['cardholder_transaction_type_de_3_1'])), 2, '0') == F.lit('20'), F.lit(1)).otherwise(F.col('lk_trx_typ_id')).cast('int').alias('TRN_TYP_ID'), F.lit(255).cast('int').alias('TRN_CYC_ID'), F.when(F.substring(F.coalesce(reversal_raw, F.lit(' ')), 1, 1) == 'R', F.lit(1)).otherwise(F.lit(0)).cast('int').alias('TRN_RVSL_FLG_ID'), crncy_lcl_ind.cast('int').alias('CRNCY_LCL_IND'), F.when(card_present != '0', F.lit(1)).otherwise(F.lit(0)).cast('int').alias('CRD_PRS_IND'), F.lit(0).cast('int').alias('ACS_FEE_IND'), F.lit(1).cast('long').alias('TRN_CNT'), F.coalesce(set_amt, F.lit(0)).alias('SET_AMT'), F.coalesce(irf_amt, F.lit(0)).alias('IRF_AMT'), F.when(card_program.isin('MSI', 'CIR'), F.lit(2)).otherwise(F.lit(1)).cast('int').alias('SUB_SCH_ID'), F.lit(1).cast('int').alias('SCH_ID'), clean_string(product_code).alias('_raw_product_code'), clean_string(first_existing_col(df, ['gcms_product_identifier'])).alias('_raw_gcms_product_identifier'), clean_string(first_existing_col(df, ['cardholder_transaction_type_de_3_1'])).alias('_raw_txn_type_code'), clean_string(first_existing_col(df, ['jurisdiction'])).alias('_raw_jurisdiction'), pds165_01.alias('_raw_pds165_01'), clean_string(raw_country).alias('_raw_country'), trx_currency.alias('_raw_transaction_currency'), card_present.alias('_raw_card_present_data'))
    return out

def add_amount_range(txn_scheme_fee: DataFrame, master_root: Any, spark: SparkSession) -> DataFrame:
    from pyspark.sql import Window
    ranges_raw = None
    last_error: Exception | None = None
    for dataset_name in ['lu_rg_mct_amt', 'LU_RG_MCT_AMT']:
        try:
            ranges_raw = read_master_dataset(spark, master_root, dataset_name)
            print(f'Using legacy amount range parquet: {dataset_name}')
            break
        except Exception as exc:
            last_error = exc
    if ranges_raw is None and (not str(master_root).lower().startswith(('s3://', 's3a://', 's3n://'))):
        csv_path = Path(str(master_root)).parent / 'csv' / 'LU_RG_MCT_AMT.csv'
        if csv_path.exists():
            ranges_raw = spark.read.option('header', 'true').option('sep', ',').option('quote', '"').option('escape', '"').csv(str(csv_path))
            print(f'Using legacy amount range CSV: {csv_path}')
    if ranges_raw is None:
        raise FileNotFoundError(f'Missing legacy amount range master lu_rg_mct_amt in report_master_root. Expected lu_rg_mct_amt/ or lu_rg_mct_amt.parquet. Last error: {last_error}')

    def _pick_col(df: DataFrame, *names: str) -> str | None:
        by_upper = {c.upper(): c for c in df.columns}
        for n in names:
            if n.upper() in by_upper:
                return by_upper[n.upper()]
        return None
    id_col = _pick_col(ranges_raw, 'RG_MCT_AMT_ID', 'MCT_AMT_RG_ID', 'APP_ID', 'SIZE_TICKET_ID')
    min_col = _pick_col(ranges_raw, 'RG_MIN', 'RG_MCT_AMT_MIN', 'SIZE_TICKET_MIN', 'RANGE_MIN')
    max_col = _pick_col(ranges_raw, 'RG_MAX', 'RG_MCT_AMT_MAX', 'SIZE_TICKET_MAX', 'RANGE_MAX')
    if not id_col or not min_col or (not max_col):
        raise ValueError(f'LU_RG_MCT_AMT columns not found. Columns={ranges_raw.columns}')
    ranges = ranges_raw.select(to_int(F.col(id_col)).alias('rg_mct_amt_id_lk'), to_decimal(F.col(min_col), 28, 6).alias('rg_min'), to_decimal(F.col(max_col), 28, 6).alias('rg_max')).where(F.col('rg_mct_amt_id_lk').isNotNull() & F.col('rg_min').isNotNull() & F.col('rg_max').isNotNull() & (F.col('rg_mct_amt_id_lk') != 255)).dropDuplicates(['rg_mct_amt_id_lk', 'rg_min', 'rg_max'])
    df = txn_scheme_fee.withColumn('_txn_row_id', F.monotonically_increasing_id())
    df = df.withColumn('SET_AMT', F.coalesce(F.col('SET_AMT').cast('decimal(28,6)'), F.lit(0).cast('decimal(28,6)')))
    df = df.withColumn('ARG_CTRY_ID', F.coalesce(F.col('ARG_CTRY_ID'), F.lit(255)))
    joined = df.join(F.broadcast(ranges), (F.col('SET_AMT') >= F.col('rg_min')) & (F.col('SET_AMT') <= F.col('rg_max')), 'left')
    w = Window.partitionBy('_txn_row_id').orderBy((F.col('rg_max') - F.col('rg_min')).asc_nulls_last(), F.col('rg_mct_amt_id_lk').asc_nulls_last())
    joined = joined.withColumn('_range_rn', F.row_number().over(w)).where(F.col('_range_rn') == 1)
    return joined.drop('MCT_AMT_RG_ID').withColumn('MCT_AMT_RG_ID', F.coalesce(F.col('rg_mct_amt_id_lk'), F.lit(255)).cast('int')).drop('rg_mct_amt_id_lk', 'rg_min', 'rg_max', '_range_rn', '_txn_row_id')

def build_txn_scheme_fee(spark: SparkSession, raw_root: Any, shared_master_root: Any, report_master_root: Any, begin_date: str, end_date: str, scheme: str='mc') -> DataFrame:
    scheme = (scheme or 'mc').lower()
    if scheme != 'mc':
        raise ValueError('This production migration is Mastercard-only. Use --scheme mc.')
    all_txn = build_mastercard_txn_scheme_fee(spark, raw_root, shared_master_root, begin_date, end_date)
    return add_amount_range(all_txn, report_master_root, spark)

def _scheme_product_program(spark: SparkSession, master_root: Any) -> DataFrame:
    raw = read_master_dataset(spark, master_root, 'scheme_fee_bin_products')
    brand_expr = F.upper(clean_string(F.col('brand'))) if 'brand' in raw.columns else F.lit('MASTERCARD')
    return raw.where(brand_expr.rlike('MC|MASTER') | brand_expr.isNull()).select(clean_string(F.col('legacy_product_id')).alias('sch_prd_id'), to_int(F.col('range_program_id')).alias('rng_prg_id')).where(F.col('sch_prd_id').isNotNull()).dropDuplicates(['sch_prd_id'])

def build_mth_acq_txn(spark: SparkSession, txn_scheme_fee: DataFrame, master_root: Any) -> DataFrame:
    base = txn_scheme_fee.where(F.col('TRN_TYP_ID').isin(1, 8)).withColumn('SET_MTH_ID', F.date_format(F.to_date(F.col('SET_DT')), 'yyyyMM').cast('int')).where(F.col('SET_MTH_ID').isNotNull()).groupBy('SET_MTH_ID', 'MCT_CD', 'ARG_FND_SRC_ID', 'ARG_CTRY_ID', 'ARG_PRD_ID', 'TRN_SCP_ID', 'TRN_TYP_ID', 'TRN_CYC_ID', 'TRN_RVSL_FLG_ID', 'CRNCY_LCL_IND', 'CRD_PRS_IND', 'ACS_FEE_IND', 'SUB_SCH_ID', 'SCH_ID', 'MCT_AMT_RG_ID').agg(F.sum(F.coalesce(F.col('TRN_CNT'), F.lit(0))).alias('TRN_CNT'), F.sum(F.coalesce(F.col('SET_AMT'), F.lit(0))).alias('SET_AMT'), F.sum(F.coalesce(F.col('IRF_AMT'), F.lit(0))).alias('IRF_AMT')).withColumn('BUS_ID', F.lit(1).cast('int')).withColumn('TXN_PRG_ID', F.lit(0).cast('int')).withColumn('RNG_REG_ID', F.lit(255).cast('int'))
    mc_program = _scheme_product_program(spark, master_root).select(F.col('sch_prd_id').alias('mc_sch_prd_id'), F.col('rng_prg_id').alias('mc_rng_prg_id'))
    base = base.join(F.broadcast(mc_program), clean_string(F.col('ARG_PRD_ID')) == F.col('mc_sch_prd_id'), 'left')
    base = base.withColumn('TXN_PRG_ID', F.when(F.col('SCH_ID') == 1, F.coalesce(F.col('mc_rng_prg_id'), F.col('TXN_PRG_ID'))).otherwise(F.col('TXN_PRG_ID')).cast('int'))
    mth = base.groupBy(F.col('SET_MTH_ID').alias('SET_MTH'), 'BUS_ID', 'SCH_ID', 'SUB_SCH_ID', F.col('TXN_PRG_ID').alias('PRG_ID'), F.col('ARG_FND_SRC_ID').alias('FND_SRC_ID'), F.col('TRN_SCP_ID').alias('TXN_SCP_ID'), F.col('TRN_TYP_ID').alias('TXN_TYP_ID'), F.col('TRN_CYC_ID').alias('TXN_CYC_ID'), F.col('TRN_RVSL_FLG_ID').alias('TXN_RVSL_FLG_ID'), F.col('CRNCY_LCL_IND').alias('TXN_CRNCY_LCL_FLG_ID'), F.col('RNG_REG_ID').alias('SCH_FEE_REG_ID'), F.col('ACS_FEE_IND').alias('TXN_ACS_FEE_FLG_ID'), F.col('CRD_PRS_IND').alias('TXN_CRD_PRS_FLG_ID'), F.col('MCT_AMT_RG_ID').alias('RG_MCT_AMT_ID'), 'MCT_CD', 'ARG_CTRY_ID').agg(F.sum('TRN_CNT').alias('TXN_CNT'), F.sum('SET_AMT').alias('TXN_AMT'), F.sum('IRF_AMT').alias('TXN_IRF'))
    return mth

FINAL_CSV_COLUMNS = ['CD', 'TXN_CRD_PRS_FLG_ID', 'SUB_SCH_LDSC', 'SCH_FEE_CAT_CD', 'SCH_FEE_CAT_DSC', 'TXN_AMT', 'TXN_CNT', 'TXN_IRF', 'TXN_PERC_IRF', 'QRT_AMT', 'QRT_FEE', 'FND_AMT', 'FND_FEE', 'AUTH_AMT', 'AUTH_FEE', 'CLR_AMT', 'CLR_FEE', 'CRSB_AMT', 'CRSB_FEE', 'OTH_AMT', 'OTH_FEE', 'TOT_PLUS_1_AMT', 'TOT_PLUS_1_FEE', 'TOT_AMT', 'TOT_FEE']
AMOUNT_COLUMNS = ['TXN_AMT', 'TXN_IRF', 'QRT_AMT', 'FND_AMT', 'AUTH_AMT', 'CLR_AMT', 'CRSB_AMT', 'OTH_AMT', 'TOT_PLUS_1_AMT', 'TOT_AMT']
FEE_COLUMNS = ['TXN_PERC_IRF', 'QRT_FEE', 'FND_FEE', 'AUTH_FEE', 'CLR_FEE', 'CRSB_FEE', 'OTH_FEE', 'TOT_PLUS_1_FEE', 'TOT_FEE']
TEMPLATE_INT_COLS = ['CTRY_ID', 'BUS_ID', 'SCH_ID', 'SUB_SCH_ID', 'TXN_SCP_ID', 'FND_SRC_ID', 'TXN_TYP_ID', 'TXN_RVSL_FLG_ID', 'TXN_CRNCY_LCL_FLG_ID', 'SCH_FEE_REG_ID', 'TXN_PRG_ID', 'TXN_ACS_FEE_FLG_ID', 'TXN_CRD_PRS_FLG_ID', 'RG_MCT_AMT_ID', 'STA']
TEMPLATE_FEE_COLS = ['FIX_QRT_FEE', 'VAR_QRT_FEE', 'FIX_FND_FEE', 'VAR_FND_FEE', 'FIX_AUTH_FEE', 'VAR_AUTH_FEE', 'FIX_CLR_FEE', 'VAR_CLR_FEE', 'FIX_CRSB_FEE', 'VAR_CRSB_FEE', 'FIX_OTH_FEE', 'VAR_OTH_FEE']

def _safe_div(num_col: str, den_col: str):
    return F.when(F.col(den_col) != 0, F.col(num_col) / F.col(den_col)).otherwise(F.lit(0))

def _decimal_to_text(value: Any, scale: int, rounding=ROUND_HALF_UP) -> str:
    if value is None:
        return ''
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value)
    quant = Decimal('1') if scale == 0 else Decimal('1.' + '0' * scale)
    return format(d.quantize(quant, rounding=rounding), f'.{scale}f')

def _value_to_text(col_name: str, value: Any) -> str:
    if value is None:
        return ''
    if col_name in AMOUNT_COLUMNS:
        return _decimal_to_text(value, 2, ROUND_HALF_UP)
    if col_name in FEE_COLUMNS:
        return _decimal_to_text(value, 6, ROUND_DOWN)
    return str(value)

def _as_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal('0')
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal('0')

def _decimal_div(num: Any, den: Any) -> Decimal:
    n = _as_decimal(num)
    d = _as_decimal(den)
    if d == 0:
        return Decimal('0')
    return n / d

def _detail_row_to_text(row: Any) -> list[str]:
    data = row.asDict() if hasattr(row, 'asDict') else dict(row)

    def _money2(value: Any) -> Decimal:
        return _as_decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    txn_amt_2 = _money2(data.get('TXN_AMT'))
    txn_irf_2 = _money2(data.get('TXN_IRF'))
    qrt_2 = _money2(data.get('QRT_AMT'))
    fnd_2 = _money2(data.get('FND_AMT'))
    auth_2 = _money2(data.get('AUTH_AMT'))
    clr_2 = _money2(data.get('CLR_AMT'))
    crsb_2 = _money2(data.get('CRSB_AMT'))
    oth_2 = _money2(data.get('OTH_AMT'))
    comp_sum_2 = qrt_2 + fnd_2 + auth_2 + clr_2 + crsb_2 + oth_2
    data['TXN_PERC_IRF'] = _decimal_div(txn_irf_2, txn_amt_2)
    data['QRT_FEE'] = _decimal_div(qrt_2, txn_amt_2)
    data['FND_FEE'] = _decimal_div(fnd_2, txn_amt_2)
    data['AUTH_FEE'] = _decimal_div(auth_2, txn_amt_2)
    data['CLR_FEE'] = _decimal_div(clr_2, txn_amt_2)
    data['CRSB_FEE'] = _decimal_div(crsb_2, txn_amt_2)
    data['OTH_FEE'] = _decimal_div(oth_2, txn_amt_2)
    data['TOT_PLUS_1_FEE'] = _decimal_div(comp_sum_2, txn_amt_2)
    data['TOT_FEE'] = _decimal_div(comp_sum_2 + txn_irf_2, txn_amt_2)
    return [_value_to_text(col, data.get(col)) for col in FINAL_CSV_COLUMNS[1:]]

def _ci_name(df: DataFrame, *names: str) -> str | None:
    by_upper = {c.upper(): c for c in df.columns}
    for name in names:
        if name.upper() in by_upper:
            return by_upper[name.upper()]
    return None

def _ci_col(df: DataFrame, *names: str, default: Any=None):
    name = _ci_name(df, *names)
    if name is None:
        return F.lit(default)
    return F.col(name)

def _read_required_master(spark: SparkSession, master_root: Any, *dataset_names: str) -> DataFrame:
    last_error: Exception | None = None
    for dataset_name in dataset_names:
        try:
            return read_master_dataset(spark, master_root, dataset_name)
        except Exception as exc:
            last_error = exc
    raise FileNotFoundError(f'Could not read any master dataset among {dataset_names}. Last error: {last_error}')

def read_lu_tmplt_scheme_fee_df(spark: SparkSession, master_root: Any) -> DataFrame:
    df = _read_required_master(spark, master_root, 'lu_tmplt_scheme_fee', 'LU_TMPLT_SCHEME_FEE')
    missing = [c for c in [*TEMPLATE_INT_COLS, 'FEE_DSC', *TEMPLATE_FEE_COLS] if _ci_name(df, c) is None]
    if missing:
        raise ValueError(f'lu_tmplt_scheme_fee is missing required columns: {missing}. Columns found: {df.columns}')
    return df.select(to_int(_ci_col(df, 'CTRY_ID')).alias('t_CTRY_ID'), to_int(_ci_col(df, 'BUS_ID')).alias('t_BUS_ID'), to_int(_ci_col(df, 'SCH_ID')).alias('t_SCH_ID'), to_int(_ci_col(df, 'SUB_SCH_ID')).alias('t_SUB_SCH_ID'), to_int(_ci_col(df, 'TXN_SCP_ID')).alias('t_TXN_SCP_ID'), to_int(_ci_col(df, 'FND_SRC_ID')).alias('t_FND_SRC_ID'), to_int(_ci_col(df, 'TXN_TYP_ID')).alias('t_TXN_TYP_ID'), to_int(_ci_col(df, 'TXN_RVSL_FLG_ID')).alias('t_TXN_RVSL_FLG_ID'), to_int(_ci_col(df, 'TXN_CRNCY_LCL_FLG_ID')).alias('t_TXN_CRNCY_LCL_FLG_ID'), to_int(_ci_col(df, 'SCH_FEE_REG_ID')).alias('t_SCH_FEE_REG_ID'), to_int(_ci_col(df, 'TXN_PRG_ID')).alias('t_TXN_PRG_ID'), to_int(_ci_col(df, 'TXN_ACS_FEE_FLG_ID')).alias('t_TXN_ACS_FEE_FLG_ID'), to_int(_ci_col(df, 'TXN_CRD_PRS_FLG_ID')).alias('t_TXN_CRD_PRS_FLG_ID'), to_int(_ci_col(df, 'RG_MCT_AMT_ID')).alias('t_RG_MCT_AMT_ID'), clean_string(_ci_col(df, 'FEE_DSC')).alias('t_FEE_DSC'), to_decimal(_ci_col(df, 'FIX_QRT_FEE', default=0), 28, 10).alias('FIX_QRT_FEE'), to_decimal(_ci_col(df, 'VAR_QRT_FEE', default=0), 28, 10).alias('VAR_QRT_FEE'), to_decimal(_ci_col(df, 'FIX_FND_FEE', default=0), 28, 10).alias('FIX_FND_FEE'), to_decimal(_ci_col(df, 'VAR_FND_FEE', default=0), 28, 10).alias('VAR_FND_FEE'), to_decimal(_ci_col(df, 'FIX_AUTH_FEE', default=0), 28, 10).alias('FIX_AUTH_FEE'), to_decimal(_ci_col(df, 'VAR_AUTH_FEE', default=0), 28, 10).alias('VAR_AUTH_FEE'), to_decimal(_ci_col(df, 'FIX_CLR_FEE', default=0), 28, 10).alias('FIX_CLR_FEE'), to_decimal(_ci_col(df, 'VAR_CLR_FEE', default=0), 28, 10).alias('VAR_CLR_FEE'), to_decimal(_ci_col(df, 'FIX_CRSB_FEE', default=0), 28, 10).alias('FIX_CRSB_FEE'), to_decimal(_ci_col(df, 'VAR_CRSB_FEE', default=0), 28, 10).alias('VAR_CRSB_FEE'), to_decimal(_ci_col(df, 'FIX_OTH_FEE', default=0), 28, 10).alias('FIX_OTH_FEE'), to_decimal(_ci_col(df, 'VAR_OTH_FEE', default=0), 28, 10).alias('VAR_OTH_FEE'), to_int(_ci_col(df, 'STA')).alias('t_STA'))

def read_lu_sub_scheme_df(spark: SparkSession, master_root: Any) -> DataFrame:
    df = _read_required_master(spark, master_root, 'lu_sub_scheme', 'LU_SUB_SCHEME')
    return df.select(to_int(_ci_col(df, 'SUB_SCH_ID')).alias('lk_SUB_SCH_ID'), clean_string(_ci_col(df, 'SUB_SCH_LDSC', 'SUB_SCH_SDSC', default='')).alias('SUB_SCH_LDSC')).dropDuplicates(['lk_SUB_SCH_ID'])

def read_lu_scheme_fee_category_df(spark: SparkSession, master_root: Any) -> DataFrame:
    df = _read_required_master(spark, master_root, 'lu_scheme_fee_category', 'LU_SCHEME_FEE_CATEGORY')
    return df.select(to_int(_ci_col(df, 'TXN_PRG_ID')).alias('lk_TXN_PRG_ID'), to_int(_ci_col(df, 'TXN_FND_SRC_ID')).alias('lk_TXN_FND_SRC_ID'), to_int(_ci_col(df, 'TXN_SCP_ID')).alias('lk_TXN_SCP_ID'), F.lpad(clean_string(_ci_col(df, 'SCH_FEE_CAT_CD', default='')), 2, '0').alias('SCH_FEE_CAT_CD'), clean_string(_ci_col(df, 'SCH_FEE_CAT_DSC', default='')).alias('SCH_FEE_CAT_DSC')).dropDuplicates(['lk_TXN_PRG_ID', 'lk_TXN_FND_SRC_ID', 'lk_TXN_SCP_ID'])

def _with_report_scope(spark: SparkSession, df: DataFrame, master_root: Any) -> DataFrame:
    try:
        country = _read_required_master(spark, master_root, 'country', 'lu_country', 'LU_COUNTRY')
        ctry = country_master_select(country, 'ARG').select(F.col('ARG_CTRY_ID').alias('_lk_ARG_CTRY_ID'), F.col('ARG_EEA_FLG_ID').alias('_lk_EEA_FLG_ID')).dropDuplicates(['_lk_ARG_CTRY_ID'])
        out = df.join(F.broadcast(ctry), to_int(F.col('ARG_CTRY_ID')) == F.col('_lk_ARG_CTRY_ID'), 'left')
        eea_col = F.when(F.col('ARG_CTRY_ID') == 90, F.lit(1)).otherwise(F.coalesce(F.col('_lk_EEA_FLG_ID'), F.lit(0)))
        return out.withColumn('RPT_TXN_SCP_ID', F.when(F.col('TXN_SCP_ID') == 1, F.lit(1)).when((F.col('TXN_SCP_ID') == 2) & (F.col('ARG_CTRY_ID') == 90), F.lit(2)).when(eea_col == 1, F.lit(4)).otherwise(F.lit(8)).cast('int')).drop('_lk_ARG_CTRY_ID', '_lk_EEA_FLG_ID')
    except Exception as exc:
        print(f'WARN: country master not available for EEA recoding; forcing ARG_CTRY_ID=90 as EEA. Error: {exc}')
        return df.withColumn('RPT_TXN_SCP_ID', F.when(F.col('TXN_SCP_ID') == 1, F.lit(1)).when((F.col('TXN_SCP_ID') == 2) & (F.col('ARG_CTRY_ID') == 90), F.lit(2)).when(F.col('ARG_CTRY_ID') == 90, F.lit(4)).otherwise(F.lit(8)).cast('int'))

def build_rpt_mct_out(spark: SparkSession, mth_acq_txn: DataFrame, shared_master_root: Any, report_master_root: Any, set_mth_id: int, rule_effective_date: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    mth = mth_acq_txn.where((F.col('SET_MTH') == int(set_mth_id)) & (F.col('TXN_TYP_ID') == 1))
    tmp_itx = mth.groupBy('SET_MTH', 'SUB_SCH_ID', 'TXN_CRD_PRS_FLG_ID', 'PRG_ID', 'FND_SRC_ID', 'TXN_SCP_ID', 'ARG_CTRY_ID').agg(F.sum('TXN_CNT').alias('TXN_CNT'), F.sum('TXN_AMT').alias('TXN_AMT'), F.sum('TXN_IRF').alias('TXN_IRF'))
    tmpl = read_lu_tmplt_scheme_fee_df(spark, report_master_root).where((F.col('t_CTRY_ID') == 90) & (F.col('t_STA') == 1) & (F.col('t_FEE_DSC') == 'Total'))
    fee_base_keys = ['SET_MTH', 'SUB_SCH_ID', 'TXN_CRD_PRS_FLG_ID', 'PRG_ID', 'FND_SRC_ID', 'TXN_SCP_ID', 'ARG_CTRY_ID', 'BUS_ID', 'SCH_ID', 'TXN_TYP_ID', 'TXN_RVSL_FLG_ID', 'TXN_CRNCY_LCL_FLG_ID', 'SCH_FEE_REG_ID', 'TXN_ACS_FEE_FLG_ID', 'RG_MCT_AMT_ID']
    mth_fee_base = mth.groupBy(*fee_base_keys).agg(F.sum('TXN_CNT').alias('TXN_CNT'), F.sum('TXN_AMT').alias('TXN_AMT'))
    t0 = mth_fee_base.alias('t0')
    t3 = tmpl.alias('t3')
    cond = (F.col('t0.BUS_ID') == F.col('t3.t_BUS_ID')) & (F.col('t0.SCH_ID') == F.col('t3.t_SCH_ID')) & ((F.col('t0.SUB_SCH_ID') == F.col('t3.t_SUB_SCH_ID')) | (F.col('t3.t_SUB_SCH_ID') == 255)) & ((F.col('t0.TXN_SCP_ID') == F.col('t3.t_TXN_SCP_ID')) | (F.col('t3.t_TXN_SCP_ID') == 255)) & ((F.col('t0.FND_SRC_ID') == F.col('t3.t_FND_SRC_ID')) | (F.col('t3.t_FND_SRC_ID') == 255)) & ((F.col('t0.TXN_TYP_ID') == F.col('t3.t_TXN_TYP_ID')) | (F.col('t3.t_TXN_TYP_ID') == 255)) & ((F.col('t0.TXN_RVSL_FLG_ID') == F.col('t3.t_TXN_RVSL_FLG_ID')) | (F.col('t3.t_TXN_RVSL_FLG_ID') == 255)) & ((F.col('t0.TXN_CRNCY_LCL_FLG_ID') == F.col('t3.t_TXN_CRNCY_LCL_FLG_ID')) | (F.col('t3.t_TXN_CRNCY_LCL_FLG_ID') == 255)) & ((F.col('t0.SCH_FEE_REG_ID') == F.col('t3.t_SCH_FEE_REG_ID')) | (F.col('t3.t_SCH_FEE_REG_ID') == 255)) & ((F.col('t0.PRG_ID') == F.col('t3.t_TXN_PRG_ID')) | (F.col('t3.t_TXN_PRG_ID') == 255)) & ((F.col('t0.TXN_ACS_FEE_FLG_ID') == F.col('t3.t_TXN_ACS_FEE_FLG_ID')) | (F.col('t3.t_TXN_ACS_FEE_FLG_ID') == 255)) & ((F.col('t0.TXN_CRD_PRS_FLG_ID') == F.col('t3.t_TXN_CRD_PRS_FLG_ID')) | (F.col('t3.t_TXN_CRD_PRS_FLG_ID') == 255)) & ((F.col('t0.RG_MCT_AMT_ID') == F.col('t3.t_RG_MCT_AMT_ID')) | (F.col('t3.t_RG_MCT_AMT_ID') == 255))
    tmp_scheme_fee = t0.join(F.broadcast(t3), cond, 'left').select(F.col('t0.SET_MTH'), F.col('t0.SUB_SCH_ID'), F.col('t0.TXN_CRD_PRS_FLG_ID'), F.col('t0.PRG_ID'), F.col('t0.FND_SRC_ID'), F.col('t0.TXN_SCP_ID'), F.col('t0.ARG_CTRY_ID'), (F.coalesce(F.col('FIX_QRT_FEE'), F.lit(0)) * F.col('t0.TXN_CNT') + F.coalesce(F.col('VAR_QRT_FEE'), F.lit(0)) * F.col('t0.TXN_AMT')).alias('QRT_AMT'), (F.coalesce(F.col('FIX_FND_FEE'), F.lit(0)) * F.col('t0.TXN_CNT') + F.coalesce(F.col('VAR_FND_FEE'), F.lit(0)) * F.col('t0.TXN_AMT')).alias('FND_AMT'), (F.coalesce(F.col('FIX_AUTH_FEE'), F.lit(0)) * F.col('t0.TXN_CNT') + F.coalesce(F.col('VAR_AUTH_FEE'), F.lit(0)) * F.col('t0.TXN_AMT')).alias('AUTH_AMT'), (F.coalesce(F.col('FIX_CLR_FEE'), F.lit(0)) * F.col('t0.TXN_CNT') + F.coalesce(F.col('VAR_CLR_FEE'), F.lit(0)) * F.col('t0.TXN_AMT')).alias('CLR_AMT'), (F.coalesce(F.col('FIX_CRSB_FEE'), F.lit(0)) * F.col('t0.TXN_CNT') + F.coalesce(F.col('VAR_CRSB_FEE'), F.lit(0)) * F.col('t0.TXN_AMT')).alias('CRSB_AMT'), (F.coalesce(F.col('FIX_OTH_FEE'), F.lit(0)) * F.col('t0.TXN_CNT') + F.coalesce(F.col('VAR_OTH_FEE'), F.lit(0)) * F.col('t0.TXN_AMT')).alias('OTH_AMT'))
    tmp_scheme_fee_upd1 = tmp_scheme_fee.groupBy('SET_MTH', 'SUB_SCH_ID', 'TXN_CRD_PRS_FLG_ID', 'PRG_ID', 'FND_SRC_ID', 'TXN_SCP_ID', 'ARG_CTRY_ID').agg(F.sum('QRT_AMT').alias('QRT_AMT'), F.sum('FND_AMT').alias('FND_AMT'), F.sum('AUTH_AMT').alias('AUTH_AMT'), F.sum('CLR_AMT').alias('CLR_AMT'), F.sum('CRSB_AMT').alias('CRSB_AMT'), F.sum('OTH_AMT').alias('OTH_AMT'))
    tmp_report_scheme_fee = tmp_itx.alias('itx').join(tmp_scheme_fee_upd1.alias('fee'), (F.col('itx.SET_MTH') == F.col('fee.SET_MTH')) & (F.col('itx.SUB_SCH_ID') == F.col('fee.SUB_SCH_ID')) & (F.col('itx.TXN_CRD_PRS_FLG_ID') == F.col('fee.TXN_CRD_PRS_FLG_ID')) & (F.col('itx.PRG_ID') == F.col('fee.PRG_ID')) & (F.col('itx.FND_SRC_ID') == F.col('fee.FND_SRC_ID')) & (F.col('itx.TXN_SCP_ID') == F.col('fee.TXN_SCP_ID')) & (F.col('itx.ARG_CTRY_ID') == F.col('fee.ARG_CTRY_ID')), 'left').select(F.col('itx.SET_MTH'), F.col('itx.SUB_SCH_ID'), F.col('itx.TXN_CRD_PRS_FLG_ID'), F.col('itx.PRG_ID'), F.col('itx.FND_SRC_ID'), F.col('itx.TXN_SCP_ID'), F.col('itx.ARG_CTRY_ID'), F.col('itx.TXN_CNT'), F.col('itx.TXN_AMT'), F.col('itx.TXN_IRF'), F.coalesce(F.col('fee.QRT_AMT'), F.lit(0)).alias('QRT_AMT'), F.coalesce(F.col('fee.FND_AMT'), F.lit(0)).alias('FND_AMT'), F.coalesce(F.col('fee.AUTH_AMT'), F.lit(0)).alias('AUTH_AMT'), F.coalesce(F.col('fee.CLR_AMT'), F.lit(0)).alias('CLR_AMT'), F.coalesce(F.col('fee.CRSB_AMT'), F.lit(0)).alias('CRSB_AMT'), F.coalesce(F.col('fee.OTH_AMT'), F.lit(0)).alias('OTH_AMT')).withColumn('TOT_PLUS_1_AMT', F.col('QRT_AMT') + F.col('FND_AMT') + F.col('AUTH_AMT') + F.col('CLR_AMT') + F.col('CRSB_AMT') + F.col('OTH_AMT')).withColumn('TOT_AMT', F.col('TOT_PLUS_1_AMT') + F.col('TXN_IRF'))
    tmp_report_scheme_fee = _with_report_scope(spark, tmp_report_scheme_fee, shared_master_root)
    rpt_mct_out = tmp_report_scheme_fee.groupBy('SET_MTH', 'SUB_SCH_ID', 'TXN_CRD_PRS_FLG_ID', 'PRG_ID', 'FND_SRC_ID', F.col('RPT_TXN_SCP_ID').alias('TXN_SCP_ID')).agg(F.sum('TXN_CNT').alias('TXN_CNT'), F.sum('TXN_AMT').alias('TXN_AMT'), F.sum('TXN_IRF').alias('TXN_IRF'), F.sum('QRT_AMT').alias('QRT_AMT'), F.sum('FND_AMT').alias('FND_AMT'), F.sum('AUTH_AMT').alias('AUTH_AMT'), F.sum('CLR_AMT').alias('CLR_AMT'), F.sum('CRSB_AMT').alias('CRSB_AMT'), F.sum('OTH_AMT').alias('OTH_AMT'), F.sum('TOT_PLUS_1_AMT').alias('TOT_PLUS_1_AMT'), F.sum('TOT_AMT').alias('TOT_AMT'))
    sub_scheme = read_lu_sub_scheme_df(spark, report_master_root)
    category = read_lu_scheme_fee_category_df(spark, report_master_root)
    report_enriched = rpt_mct_out.alias('r').join(F.broadcast(sub_scheme).alias('s'), F.col('r.SUB_SCH_ID') == F.col('s.lk_SUB_SCH_ID'), 'left').join(F.broadcast(category).alias('c'), (F.col('r.PRG_ID') == F.col('c.lk_TXN_PRG_ID')) & (F.col('r.FND_SRC_ID') == F.col('c.lk_TXN_FND_SRC_ID')) & (F.col('r.TXN_SCP_ID') == F.col('c.lk_TXN_SCP_ID')), 'left')
    report_upd = report_enriched.groupBy('TXN_CRD_PRS_FLG_ID', 'SUB_SCH_LDSC', 'SCH_FEE_CAT_CD', 'SCH_FEE_CAT_DSC').agg(F.sum('TXN_AMT').alias('TXN_AMT'), F.sum('TXN_CNT').alias('TXN_CNT'), F.sum('TXN_IRF').alias('TXN_IRF'), F.sum('QRT_AMT').alias('QRT_AMT'), F.sum('FND_AMT').alias('FND_AMT'), F.sum('AUTH_AMT').alias('AUTH_AMT'), F.sum('CLR_AMT').alias('CLR_AMT'), F.sum('CRSB_AMT').alias('CRSB_AMT'), F.sum('OTH_AMT').alias('OTH_AMT'), F.sum('TOT_PLUS_1_AMT').alias('TOT_PLUS_1_AMT'), F.sum('TOT_AMT').alias('TOT_AMT')).withColumn('TXN_PERC_IRF', _safe_div('TXN_IRF', 'TXN_AMT')).withColumn('QRT_FEE', _safe_div('QRT_AMT', 'TXN_AMT')).withColumn('FND_FEE', _safe_div('FND_AMT', 'TXN_AMT')).withColumn('AUTH_FEE', _safe_div('AUTH_AMT', 'TXN_AMT')).withColumn('CLR_FEE', _safe_div('CLR_AMT', 'TXN_AMT')).withColumn('CRSB_FEE', _safe_div('CRSB_AMT', 'TXN_AMT')).withColumn('OTH_FEE', _safe_div('OTH_AMT', 'TXN_AMT')).withColumn('TOT_PLUS_1_FEE', _safe_div('TOT_PLUS_1_AMT', 'TXN_AMT')).withColumn('TOT_FEE', _safe_div('TOT_AMT', 'TXN_AMT')).withColumn('TXN_CRD_PRS_FLG_ID', F.when(F.col('TXN_CRD_PRS_FLG_ID') == 0, F.lit(1)).when(F.col('TXN_CRD_PRS_FLG_ID') == 1, F.lit(0)).otherwise(F.lit(None).cast('int'))).select(*FINAL_CSV_COLUMNS[1:])
    return (rpt_mct_out, report_upd, tmpl)

def write_final_report_csv(report_upd: DataFrame, set_mth_id: int, output_csv: Path | str) -> dict[str, Any]:
    output_csv_s = str(output_csv)
    ensure_parent_dir(output_csv_s)
    detail_rows = [row.asDict() for row in report_upd.orderBy('TXN_CRD_PRS_FLG_ID', 'SUB_SCH_LDSC', 'SCH_FEE_CAT_CD').collect()]
    nul = '\x00'
    rows: list[list[str]] = []
    rows.append(['01', str(set_mth_id)] + [nul] * 23)
    for row in detail_rows:
        rows.append(['02'] + _detail_row_to_text(row))
    detail_text_rows = rows[1:]

    def _sum_text_col(col_idx: int) -> Decimal:
        total_value = Decimal('0')
        for out_row in detail_text_rows:
            value = out_row[col_idx]
            if value not in ('', nul):
                total_value += Decimal(str(value))
        return total_value
    total_txn_cnt = sum((int(out_row[6]) for out_row in detail_text_rows if out_row[6] not in ('', nul)), 0)
    rows.append(['03', str(len(detail_rows)), nul, nul, nul, _decimal_to_text(_sum_text_col(5), 2, ROUND_HALF_UP), str(total_txn_cnt), _decimal_to_text(_sum_text_col(7), 2, ROUND_HALF_UP), _decimal_to_text(_sum_text_col(8), 6, ROUND_DOWN), _decimal_to_text(_sum_text_col(9), 2, ROUND_HALF_UP), _decimal_to_text(_sum_text_col(10), 6, ROUND_DOWN), _decimal_to_text(_sum_text_col(11), 2, ROUND_HALF_UP), _decimal_to_text(_sum_text_col(12), 6, ROUND_DOWN), _decimal_to_text(_sum_text_col(13), 2, ROUND_HALF_UP), _decimal_to_text(_sum_text_col(14), 6, ROUND_DOWN), _decimal_to_text(_sum_text_col(15), 2, ROUND_HALF_UP), _decimal_to_text(_sum_text_col(16), 6, ROUND_DOWN), _decimal_to_text(_sum_text_col(17), 2, ROUND_HALF_UP), _decimal_to_text(_sum_text_col(18), 6, ROUND_DOWN), _decimal_to_text(_sum_text_col(19), 2, ROUND_HALF_UP), _decimal_to_text(_sum_text_col(20), 6, ROUND_DOWN), _decimal_to_text(_sum_text_col(21), 2, ROUND_HALF_UP), _decimal_to_text(_sum_text_col(22), 6, ROUND_DOWN), _decimal_to_text(_sum_text_col(23), 2, ROUND_HALF_UP), _decimal_to_text(_sum_text_col(24), 6, ROUND_DOWN)])
    buffer = io.StringIO(newline='')
    writer = csv.writer(buffer, delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_MINIMAL, quotechar='"', doublequote=True, escapechar='\\')
    writer.writerows(rows)
    csv_bytes = buffer.getvalue().encode('cp1252', errors='replace')
    if is_s3_uri(output_csv_s):
        import boto3
        bucket, key = split_s3_uri(output_csv_s)
        boto3.client('s3').put_object(Bucket=bucket, Key=key, Body=csv_bytes, ContentType='text/csv')
        written_size = len(csv_bytes)
    else:
        output_path = Path(output_csv_s)
        output_path.write_bytes(csv_bytes)
        written_size = output_path.stat().st_size
    if written_size == 0:
        raise RuntimeError(f'Final CSV was not written or is empty: {output_csv_s}')
    return {'final_csv_path': output_csv_s, 'final_csv_rows': len(rows), 'final_csv_detail_rows': len(detail_rows), 'final_csv_columns': FINAL_CSV_COLUMNS}

DEFAULTS: dict[str, Any] = {'JOB_NAME': 'eurobank-merchant-report', 'raw_root': None, 'output_root': None, 'shared_master_root': SHARED_MASTER_ROOT, 'report_master_root': REPORT_MASTER_ROOT}


def _arg_is_present(name: str) -> bool:
    return f'--{name}' in sys.argv

def _parse_args() -> argparse.Namespace:
    if AWS_GLUE_AVAILABLE:
        required = ['JOB_NAME', 'begin_date', 'end_date']
        optional = ['raw_root', 'output_root', 'shared_master_root', 'report_master_root']
        present_optional = [name for name in optional if _arg_is_present(name)]
        resolved = getResolvedOptions(sys.argv, required + present_optional)
        merged = dict(DEFAULTS)
        merged.update(resolved)
        missing_config = [name for name in ['raw_root', 'output_root', 'shared_master_root', 'report_master_root'] if not merged.get(name)]
        if missing_config:
            raise ValueError('Missing required Glue configuration. Configure these as Glue Job Default arguments: ' + ', '.join((f'--{name}' for name in missing_config)))
        return argparse.Namespace(**merged)
    parser = argparse.ArgumentParser(description='Eurobank Merchant Report - AWS Glue production job')
    parser.add_argument('--JOB_NAME', default=DEFAULTS['JOB_NAME'])
    parser.add_argument('--begin_date', '--begin-date', dest='begin_date', required=True)
    parser.add_argument('--end_date', '--end-date', dest='end_date', required=True)
    parser.add_argument('--raw_root', '--raw-root', dest='raw_root', required=True)
    parser.add_argument('--output_root', '--output-root', dest='output_root', required=True)
    parser.add_argument('--shared_master_root', '--shared-master-root', dest='shared_master_root', default=DEFAULTS['shared_master_root'])
    parser.add_argument('--report_master_root', '--report-master-root', dest='report_master_root', default=DEFAULTS['report_master_root'])
    known, _unknown = parser.parse_known_args(sys.argv[1:])
    return known

def _create_spark_and_glue_job(args: argparse.Namespace) -> tuple[SparkSession, Any | None]:
    if AWS_GLUE_AVAILABLE:
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        spark.conf.set('spark.sql.parquet.mergeSchema', 'false')
        spark.conf.set('spark.sql.shuffle.partitions', '32')
        spark.conf.set('spark.sql.adaptive.enabled', 'true')
        glue_job = Job(glue_context)
        glue_args = {k: str(v) for k, v in vars(args).items() if v is not None}
        glue_job.init(args.JOB_NAME, glue_args)
        return (spark, glue_job)
    spark = SparkSession.builder.appName(args.JOB_NAME).config('spark.sql.session.timeZone', 'UTC').config('spark.sql.parquet.mergeSchema', 'false').config('spark.sql.shuffle.partitions', '32').config('spark.sql.adaptive.enabled', 'true').getOrCreate()
    return (spark, None)

def main() -> None:
    args = _parse_args()
    set_mth_id = int(args.begin_date[:7].replace('-', ''))
    output_root = join_path(args.output_root, str(set_mth_id))
    spark, glue_job = _create_spark_and_glue_job(args)
    try:
        print('Eurobank Merchant Report - AWS Glue production')
        print('begin_date:', args.begin_date)
        print('end_date:', args.end_date)
        print('set_mth_id:', set_mth_id)
        print('scheme:', 'mc')
        print('raw_root:', args.raw_root)
        print('shared_master_root:', args.shared_master_root)
        print('report_master_root:', args.report_master_root)
        print('output_root:', output_root)
        print('aws_glue_runtime:', AWS_GLUE_AVAILABLE)
        txn_scheme_fee = build_txn_scheme_fee(spark=spark, raw_root=args.raw_root, shared_master_root=args.shared_master_root, report_master_root=args.report_master_root, begin_date=args.begin_date, end_date=args.end_date, scheme='mc').persist(StorageLevel.DISK_ONLY)
        mth_acq_txn = build_mth_acq_txn(spark, txn_scheme_fee, args.shared_master_root).persist(StorageLevel.DISK_ONLY)
        _rpt_mct_out, report_upd, _rule_join_sample = build_rpt_mct_out(spark=spark, mth_acq_txn=mth_acq_txn, shared_master_root=args.shared_master_root, report_master_root=args.report_master_root, set_mth_id=set_mth_id, rule_effective_date=args.end_date)
        final_csv_path = join_path(output_root, f'RPT_MCT_{set_mth_id}.csv')
        final_csv_meta = write_final_report_csv(report_upd, set_mth_id, final_csv_path)
        print('Final CSV written:', final_csv_meta['final_csv_path'])
        print('Final CSV rows:', final_csv_meta['final_csv_rows'])
        print('Final CSV detail rows:', final_csv_meta['final_csv_detail_rows'])
        print('Job completed successfully')
        if glue_job is not None:
            glue_job.commit()
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
