from src import config
from src.utils import normalize_column_names
import logging

import polars as pl
import pandas as pd
import s3fs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

logger = logging.getLogger(__name__)

def run():
    logger.info("--- Running Reclamacoes ETL ---")

    fs = s3fs.S3FileSystem(
        key=config.STORAGE_OPTIONS["aws_access_key_id"],
        secret=config.STORAGE_OPTIONS["aws_secret_access_key"],
        endpoint_url=config.STORAGE_OPTIONS["aws_endpoint_url"]
    )

    source_path = f"{config.S3_BUCKET}/raw-data/reclamacoes/*.csv"

    file_paths = fs.glob(source_path)

    if file_paths:
        logger.info(f"Found {len(file_paths)} files in {source_path}")

        # First read the CSV with pandas with the correct encoding
        dfs = []
        for file_path in file_paths:
            file_size = fs.info(file_path).get('Size', 0)
            logger.info(f"Reading file: {file_path} with size {file_size} bytes")
            # Check if the file is not empty
            if file_size > 0:
                with fs.open(file_path, 'rb') as f:
                    df = pd.read_csv(f, sep=';', encoding='iso-8859-1', dtype=str)
                    df = df.rename(columns=normalize_column_names(df.columns))

                    dfs.append(df)

                    logger.info(f"Read {len(df)} rows from {file_path}")
            else:
                logger.warning(f"File {file_path} is empty, skipping.")

        final_df = pd.concat(dfs, ignore_index=True)

        logger.info(f"Total rows after concatenation: {len(final_df)}")

        df_polars = pl.from_pandas(final_df)

        df_polars = df_polars \
        .with_columns(
            pl.when(pl.col("CNPJ_IF").str.strip_chars() != pl.lit('')) \
            .then(pl.col('CNPJ_IF').cast(pl.Utf8).str.zfill(8)) \
            .alias("CNPJ_IF"),
            pl.col("QUANTIDADE_DE_CLIENTES_CCS").str.strip_chars().replace("", None).cast(pl.Int64).alias("QUANTIDADE_DE_CLIENTES_CCS"),
            pl.col("QUANTIDADE_DE_CLIENTES_SCR").str.strip_chars().replace("", None).cast(pl.Int64).alias("QUANTIDADE_DE_CLIENTES_SCR"),
            pl.col("QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR").str.strip_chars().replace("", None).cast(pl.Int64).alias("QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR"),
        )

        with fs.open(f"{config.S3_BUCKET}/trust-data/reclamacoes/reclamacoes.parquet", 'wb') as f:
            df_polars.write_parquet(f)

        logger.info(f"Reclamacoes ETL complete. Saved {len(df_polars)} rows.")
        
    else:
        logger.warning(f"No files found in {config.RAW_DATA_PATH}/reclamacoes/")
