# Exemplo de Ingestão de Dados 2 (Local)

Este projeto oferece um pipeline modular de ingestão de dados para ambientes locais e em nuvem. Utiliza [Polars](https://pola.rs/), [PostgreSQL](https://www.postgresql.org/), [LocalStack](https://github.com/localstack/localstack) e [Docker Compose](https://docs.docker.com/compose/) para processar, limpar e entregar dados de arquivos brutos para armazenamento confiável e banco de dados. O código é organizado para facilitar manutenção e extensibilidade, adequado para fluxos de trabalho de produção.

## Como Configurar o Ambiente

1. **Clone o repositório**
   ```
   git clone https://github.com/eEDB-Grupo-5/data_ingestion_ex_2_local.git
   cd data_ingestion_ex_2_local
   ```

2. **Instale as dependências Python**
   ```
   pip install -r src/requirements.txt
   ```

3. **Inicie os serviços com Docker Compose**
   ```
   docker-compose up --build
   ```

4. **Acesse o JupyterLab**
   Abra [http://localhost:8888](http://localhost:8888) no seu navegador.

5. **Configure conexões AWS e banco de dados**
   Certifique-se de que suas variáveis de ambiente e arquivos de configuração estejam ajustados para conectar ao S3 e PostgreSQL. Consulte [`docker-compose.yml`](docker-compose.yml) e [`src/config`](src/config) para configuração dos serviços.

6. **Adicione arquivos de dados brutos**
   Coloque os dados brutos nos diretórios `local-raw-data/` ou `data/raw-data/` conforme necessário pelos scripts do pipeline.

## Como Executar o Pipeline

- **Scripts Python:**  
  Execute os módulos do pipeline diretamente pelo terminal ou dentro do JupyterLab:
  ```
  python src/pipelines/process_bancos.py
  python src/pipelines/trusted_to_delivery.py
  ```
  Alternativamente, importe e chame a função `run()` em um notebook.

- **DAGs do Airflow:**  
  Se utilizar Airflow, coloque os DAGs no diretório `dags/`. O webserver do Airflow irá detectar e agendar automaticamente.

## Como Acessar o pgAdmin

- Abra [http://localhost:5050](http://localhost:5050) no seu navegador.
- Faça login usando suas credenciais configuradas.
- O servidor PostgreSQL estará disponível para inspeção e gerenciamento dos resultados do pipeline.

## Técnicas Utilizadas

- **Processamento LazyFrame:** Transformações eficientes e paralelas usando [Polars LazyFrame](https://pola.rs/reference/lazyframe/).
- **Integração com S3:** Interação direta com armazenamento compatível com S3 via [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) e [s3fs](https://s3fs.readthedocs.io/en/latest/).
- **Stack Local de Serviços Cloud:** Emulação de serviços AWS com [LocalStack](https://github.com/localstack/localstack).
- **Carga Automatizada no Banco:** Integração com PostgreSQL usando [psycopg2](https://www.psycopg.org/docs/).
- **Logging Estruturado:** Logs consistentes e específicos por arquivo usando [logging](https://docs.python.org/3/library/logging.html) do Python.
- **Modularidade ETL:** Estágios do pipeline separados para clareza e testabilidade.

## Bibliotecas e Tecnologias de Destaque

- [Polars](https://pola.rs/)
- [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)
- [s3fs](https://s3fs.readthedocs.io/en/latest/)
- [LocalStack](https://github.com/localstack/localstack)
- [psycopg2](https://www.psycopg.org/docs/)
- [JupyterLab](https://jupyter.org/)
- [Docker Compose](https://docs.docker.com/compose/)