from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="3_reclamacoes_raw_to_trusted",
    schedule=None, start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False, tags=["source", "trusted"],
) as dag:
    run_reclamacoes_etl = BashOperator(
        task_id="run_reclamacoes_etl",
        bash_command="python /opt/airflow/src/main.py --step reclamacoes",
    )
