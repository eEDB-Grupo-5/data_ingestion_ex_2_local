from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="1_bancos_raw_to_trusted",
    schedule=None, start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False, tags=["source", "trusted"],
) as dag:
    run_bancos_etl = BashOperator(
        task_id="run_bancos_etl",
        bash_command="python /opt/airflow/src/main.py --step bancos",
    )
