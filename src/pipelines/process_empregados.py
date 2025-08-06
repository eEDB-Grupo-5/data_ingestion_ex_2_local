from src import config
from src.utils import normalize_column_names
import logging

import polars as pl
import s3fs
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

logger = logging.getLogger(__name__)

def run():
    logger.info("--- Running Empregados ETL ---")

    lf_empregados = pl.scan_csv(
        f"{config.RAW_DATA_PATH}/empregados/glassdoor_consolidado_join_match_v2.csv", 
        separator='|', 
        encoding='utf8', 
        storage_options=config.STORAGE_OPTIONS,
        ignore_errors=True 
    )

    lf_empregados = lf_empregados.rename(normalize_column_names(lf_empregados.columns))

    lf_empregados_less = pl.scan_csv(
        f"{config.RAW_DATA_PATH}/empregados/glassdoor_consolidado_join_match_less_v2.csv", 
        separator='|', 
        encoding='utf8', 
        storage_options=config.STORAGE_OPTIONS,
        ignore_errors=True 
    )

    lf_empregados_less = lf_empregados_less.rename(normalize_column_names(lf_empregados_less.columns))

    lf_empregados_complete = pl.concat([lf_empregados, lf_empregados_less], how='diagonal')

    # normalize CNPJ to have 8 digits with leading zeros
    lf_empregados_complete = lf_empregados_complete \
    .with_columns(
        pl.col('CNPJ').cast(pl.Utf8).str.zfill(8).alias("CNPJ")
    )
    
    # remove duplicates
    lf_empregados_complete = lf_empregados_complete.unique()

    df = lf_empregados_complete.collect()

    print(df.columns)

    fs = s3fs.S3FileSystem(
        key=config.STORAGE_OPTIONS["aws_access_key_id"],
        secret=config.STORAGE_OPTIONS["aws_secret_access_key"],
        endpoint_url=config.STORAGE_OPTIONS["aws_endpoint_url"]
    )

    with fs.open(f"{config.S3_BUCKET}/trust-data/empregados/empregados.parquet", 'wb') as f:
        df.write_parquet(f)

    logger.info(f"Empregados ETL complete. Saved {len(df)} rows.")
