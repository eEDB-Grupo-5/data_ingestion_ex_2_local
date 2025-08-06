from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="0_master_orchestrator",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    doc_md="""
    ### Master Orchestrator DAG
    This DAG triggers all the individual ETL pipelines in the correct order.
    1. Triggers the three source-to-trusted DAGs in parallel.
    2. After they all succeed, it triggers the final trusted-to-delivery join DAG.
    """,
    tags=["orchestrator"],
) as dag:

    with TaskGroup(group_id="trigger_source_etl_dags") as trigger_source_dags:
        trigger_bancos_etl = TriggerDagRunOperator(
            task_id="trigger_bancos_etl",
            trigger_dag_id="1_bancos_raw_to_trusted", 
            wait_for_completion=True,
            deferrable=True,
            poke_interval=10,
            failed_states=["failed"],
        )

        # Task to trigger the 'empregados' ETL DAG
        trigger_empregados_etl = TriggerDagRunOperator(
            task_id="trigger_empregados_etl",
            trigger_dag_id="2_empregados_raw_to_trusted",
            wait_for_completion=True,
            deferrable=True,
            poke_interval=10,
            failed_states=["failed"],
        )

        # Task to trigger the 'reclamacoes' ETL DAG
        trigger_reclamacoes_etl = TriggerDagRunOperator(
            task_id="trigger_reclamacoes_etl",
            trigger_dag_id="3_reclamacoes_raw_to_trusted",
            wait_for_completion=True,
            deferrable=True,
            poke_interval=10,
            failed_states=["failed"],
        )

    # Task to trigger the final join-and-load DAG
    trigger_join_and_load_etl = TriggerDagRunOperator(
        task_id="trigger_join_and_load_etl",
        trigger_dag_id="4_trusted_to_delivery",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=10,
        failed_states=["failed"],
    )

    trigger_source_dags >> trigger_join_and_load_etl
