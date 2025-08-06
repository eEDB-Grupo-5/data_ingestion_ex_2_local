import polars as pl
import psycopg2
from psycopg2.extensions import connection
from src import config
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def _load_to_postgres(df: pl.DataFrame, conn: connection):
    """Helper function to load a dataframe into the database."""
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {config.DB_TARGET_TABLE};")
        create_table_query = f"""
        CREATE TABLE {config.DB_TARGET_TABLE} (
        id_empregado BIGINT,
        nome_empregado TEXT,
        codigo_banco BIGINT,
        nome_banco TEXT,
        reclamacao TEXT
        );
    """
    cursor.execute(create_table_query)
    # Use a temporary in-memory buffer for efficient loading
    buffer = df.to_pandas().to_csv(index=False, header=True)
    cursor.copy_expert(f"COPY {config.DB_TARGET_TABLE} FROM STDIN WITH CSV HEADER", buffer)
    conn.commit()
    logging.info(f"Successfully loaded {len(df)} rows into '{config.DB_TARGET_TABLE}'.")

def run():
    """
    Reads trusted Parquet files, joins them, and loads the final result
    to Postgres and the delivery S3 location.
    """
    logging.info("====== STAGE 2: Processing Trusted to Delivery Zone ======")

    # 1. Read from Trusted Zone
    logging.info("Reading from trusted zone...")
    bancos = pl.read_parquet(f"{config.TRUSTED_DATA_PATH}/bancos/bancos.parquet", storage_options=config.STORAGE_OPTIONS)
    empregados = pl.read_parquet(f"{config.TRUSTED_DATA_PATH}/empregados/empregados.parquet", storage_options=config.STORAGE_OPTIONS)
    reclamacoes = pl.read_parquet(f"{config.TRUSTED_DATA_PATH}/reclamacoes/reclamacoes.parquet", storage_options=config.STORAGE_OPTIONS)

    # # 2. Join the dataframes
    # logging.info("Joining data...")
    # df_joined = empregados.join(bancos, on="codigo_banco", how="left")
    # df_final = df_joined.join(reclamacoes, on="id_empregado", how="left")
    # df_final = df_final.with_columns(pl.col("reclamacao").fill_null("Nenhuma reclamação registrada"))

    # # 3. Load to Delivery S3 location
    # logging.info("Saving final report to delivery S3 path...")
    # df_final.write_parquet(f"{config.DELIVERY_DATA_PATH}/relatorio_consolidado.parquet", storage_options=config.STORAGE_OPTIONS)
    # logging.info(f"Saved {len(df_final)} final rows to delivery-data/relatorio_consolidado.parquet")

    # # 4. Load to PostgreSQL
    # logging.info("Loading final report to PostgreSQL...")
    # try:
    #     conn = psycopg2.connect(
    #         host=config.DB_HOST,
    #         dbname=config.DB_NAME,
    #         user=config.DB_USER,
    #         password=config.DB_PASSWORD,
    #         port=config.DB_PORT
    #     )
    #     _load_to_postgres(df_final, conn)
    # finally:
    #     if 'conn' in locals() and conn:
    #         conn.close()
    #         logging.info("PostgreSQL connection closed.")

    logging.info("====== STAGE 2: Complete ======")