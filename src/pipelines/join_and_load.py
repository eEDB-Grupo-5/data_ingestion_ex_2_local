import logging
from io import StringIO
from src import config
from src.utils import map_polars_to_postgres_types

import polars as pl
import psycopg2
from psycopg2.extensions import connection
import s3fs


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

logger = logging.getLogger(__name__)

def _load_to_postgres(df: pl.DataFrame, conn: connection, table_name: str, create_table_query: str = None):
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        cursor.execute(create_table_query)

        buffer = StringIO()
        df.write_csv(buffer, include_header=True)
        buffer.seek(0)  # Reset buffer position to the beginning
         
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", buffer)
        conn.commit()
        logging.info(f"Successfully loaded {len(df)} rows into '{table_name}'.")

def run():
    print("--- Running Final Join and Load ETL ---")
    bancos = pl.read_parquet(f"{config.TRUSTED_DATA_PATH}/bancos/bancos.parquet", storage_options=config.STORAGE_OPTIONS)
    empregados = pl.read_parquet(f"{config.TRUSTED_DATA_PATH}/empregados/empregados.parquet", storage_options=config.STORAGE_OPTIONS)
    reclamacoes = pl.read_parquet(f"{config.TRUSTED_DATA_PATH}/reclamacoes/reclamacoes.parquet", storage_options=config.STORAGE_OPTIONS)

    df_bancos_empregados_1 = bancos.join(
        empregados,
        left_on="CNPJ",
        right_on="CNPJ",
        how="left"
    ).select(
        [
            'SEGMENTO', 
            'CNPJ', 
            'NOME', 
            'EMPLOYER_NAME', 
            'REVIEWS_COUNT', 
            'CULTURE_COUNT', 
            'SALARIES_COUNT', 
            'BENEFITS_COUNT', 
            'EMPLOYERWEBSITE', 
            'EMPLOYERHEADQUARTERS', 
            'EMPLOYERFOUNDED', 
            'EMPLOYERINDUSTRY',
            'EMPLOYERREVENUE', 
            'URL', 
            'GERAL', 
            'CULTURA_E_VALORES', 
            'DIVERSIDADE_E_INCLUSAO', 
            'QUALIDADE_DE_VIDA', 
            'ALTA_LIDERANCA', 
            'REMUNERACAO_E_BENEFICIOS', 
            'OPORTUNIDADES_DE_CARREIRA', 
            'RECOMENDAM_PARA_OUTRAS_PESSOAS', 
            'PERSPECTIVA_POSITIVA_DA_EMPRESA', 
            'MATCH_PERCENT'
        ]
    )

    df_bancos_empregados_2 = bancos.join(
        empregados,
        left_on=['SEGMENTO', 'NOME'],
        right_on=['SEGMENTO', 'NOME'],
        how="left"
    ).select(
        [
            'SEGMENTO', 
            'CNPJ', 
            'NOME', 
            'EMPLOYER_NAME', 
            'REVIEWS_COUNT', 
            'CULTURE_COUNT', 
            'SALARIES_COUNT', 
            'BENEFITS_COUNT', 
            'EMPLOYERWEBSITE', 
            'EMPLOYERHEADQUARTERS', 
            'EMPLOYERFOUNDED', 
            'EMPLOYERINDUSTRY',
            'EMPLOYERREVENUE', 
            'URL', 
            'GERAL', 
            'CULTURA_E_VALORES', 
            'DIVERSIDADE_E_INCLUSAO', 
            'QUALIDADE_DE_VIDA', 
            'ALTA_LIDERANCA', 
            'REMUNERACAO_E_BENEFICIOS', 
            'OPORTUNIDADES_DE_CARREIRA', 
            'RECOMENDAM_PARA_OUTRAS_PESSOAS', 
            'PERSPECTIVA_POSITIVA_DA_EMPRESA', 
            'MATCH_PERCENT'
        ]
    )

    df_bancos_empregados = pl.concat([df_bancos_empregados_1, df_bancos_empregados_2]).unique()

    df_reclamacoes = reclamacoes.filter(
        pl.col("CNPJ_IF") != pl.lit('')
    )

    df_final = df_bancos_empregados.join(
        df_reclamacoes,
        left_on="CNPJ",
        right_on="CNPJ_IF",
        how="left"
    ).select(
        [
            'SEGMENTO', 
            'CNPJ', 
            'NOME', 
            'EMPLOYER_NAME', 
            'REVIEWS_COUNT', 
            'CULTURE_COUNT', 
            'SALARIES_COUNT', 
            'BENEFITS_COUNT', 
            'EMPLOYERWEBSITE', 
            'EMPLOYERHEADQUARTERS', 
            'EMPLOYERFOUNDED', 
            'EMPLOYERINDUSTRY',
            'EMPLOYERREVENUE', 
            'URL', 
            'GERAL', 
            'CULTURA_E_VALORES', 
            'DIVERSIDADE_E_INCLUSAO', 
            'QUALIDADE_DE_VIDA', 
            'ALTA_LIDERANCA', 
            'REMUNERACAO_E_BENEFICIOS', 
            'OPORTUNIDADES_DE_CARREIRA', 
            'RECOMENDAM_PARA_OUTRAS_PESSOAS', 
            'PERSPECTIVA_POSITIVA_DA_EMPRESA', 
            'MATCH_PERCENT',
            'ANO', 
            'TRIMESTRE', 
            'CATEGORIA', 
            'TIPO', 
            'INSTITUICAO_FINANCEIRA', 
            'INDICE', 
            'QUANTIDADE_DE_RECLAMACOES_REGULADAS_PROCEDENTES', 
            'QUANTIDADE_DE_RECLAMACOES_REGULADAS_OUTRAS', 
            'QUANTIDADE_DE_RECLAMACOES_NAO_REGULADAS', 
            'QUANTIDADE_TOTAL_DE_RECLAMACOES', 
            'QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR', 
            'QUANTIDADE_DE_CLIENTES_CCS', 
            'QUANTIDADE_DE_CLIENTES_SCR'
        ]
    ).cast(
        {
            "NOME": pl.Utf8,
            "REVIEWS_COUNT": pl.Int64,
            "CULTURE_COUNT": pl.Int64,
            "SALARIES_COUNT": pl.Int64,
            "BENEFITS_COUNT": pl.Int64,
            "EMPLOYERFOUNDED": pl.Int64,
            "GERAL": pl.Float64,
            "CULTURA_E_VALORES": pl.Float64,
            "DIVERSIDADE_E_INCLUSAO": pl.Float64,
            "QUALIDADE_DE_VIDA": pl.Float64,
            "ALTA_LIDERANCA": pl.Float64,
            "REMUNERACAO_E_BENEFICIOS": pl.Float64,
            "OPORTUNIDADES_DE_CARREIRA": pl.Float64,
            "RECOMENDAM_PARA_OUTRAS_PESSOAS": pl.Float64,
            "PERSPECTIVA_POSITIVA_DA_EMPRESA": pl.Float64,
            "MATCH_PERCENT": pl.Float64,
            "ANO": pl.Int64,
            "QUANTIDADE_DE_RECLAMACOES_REGULADAS_PROCEDENTES": pl.Int64,
            "QUANTIDADE_DE_RECLAMACOES_REGULADAS_OUTRAS": pl.Int64,
            "QUANTIDADE_DE_RECLAMACOES_NAO_REGULADAS": pl.Int64,
            "QUANTIDADE_TOTAL_DE_RECLAMACOES": pl.Int64,
            "QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR": pl.Int64,
            "QUANTIDADE_DE_CLIENTES_CCS": pl.Int64,
            "QUANTIDADE_DE_CLIENTES_SCR": pl.Int64
        }
    )

    print(f"Total rows after join: {len(df_final)}")

    fs = s3fs.S3FileSystem(
        key=config.STORAGE_OPTIONS["aws_access_key_id"],
        secret=config.STORAGE_OPTIONS["aws_secret_access_key"],
        endpoint_url=config.STORAGE_OPTIONS["aws_endpoint_url"]
    )

    destination_path = f"{config.S3_BUCKET}/delivery-data/joined_data.parquet"

    with fs.open(destination_path, 'wb') as f:
        df_final.write_parquet(f)

    logging.info(f"Saved {len(df_final)} final rows to {destination_path}")

    # Load to Postgres
    logging.info("Loading final report to PostgreSQL...")
    target_table_name = "JOINED_EMPREGADOS_BANCOS_RECLAMACOES"

    column_definitions = [f'"{col_name}" {map_polars_to_postgres_types(dtype)}' for col_name, dtype in df_final.schema.items()]

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {target_table_name} (
        {', '.join(column_definitions)}
    );
    """
    
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            port=config.DB_PORT
        )

        _load_to_postgres(
            df=df_final, 
            conn=conn,
            table_name=target_table_name,
            create_table_query=create_table_sql
        )

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("PostgreSQL connection closed.")
