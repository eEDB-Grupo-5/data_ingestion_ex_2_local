from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="4_trusted_to_delivery",
    schedule=None, start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False, tags=["delivery"],
) as dag:
    run_join_and_load = BashOperator(
        task_id="run_join_and_load",
        bash_command="python /opt/airflow/src/main.py --step join_and_load",
    )
