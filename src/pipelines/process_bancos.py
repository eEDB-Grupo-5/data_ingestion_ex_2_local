from src import config
from src.utils import normalize_column_names
import logging

import polars as pl
import s3fs
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

logger = logging.getLogger(__name__)

def run():
    logger.info("--- Running Bancos ETL ---")

    lf_bancos = pl.scan_csv(
        f"{config.RAW_DATA_PATH}/bancos/*.tsv", 
        separator='\t', 
        encoding='utf8', 
        storage_options=config.STORAGE_OPTIONS,
        ignore_errors=True 
    )

    lf_bancos = lf_bancos.rename(normalize_column_names(lf_bancos.columns))
    
    # normalize CNPJ to have 8 digits with leading zeros
    lf_bancos = lf_bancos \
    .with_columns(
        pl.col('CNPJ').cast(pl.Utf8).str.zfill(8).alias("CNPJ")
    )

    # remove duplicates
    lf_bancos = lf_bancos.unique()

    df = lf_bancos.collect()

    fs = s3fs.S3FileSystem(
        key=config.STORAGE_OPTIONS["aws_access_key_id"],
        secret=config.STORAGE_OPTIONS["aws_secret_access_key"],
        endpoint_url=config.STORAGE_OPTIONS["aws_endpoint_url"]
    )

    with fs.open(f"{config.S3_BUCKET}/trust-data/bancos/bancos.parquet", 'wb') as f:
        df.write_parquet(f)

    logger.info(f"Bancos ETL complete. Saved {len(df)} rows.")
