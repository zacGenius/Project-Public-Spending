# pipelines/orchestration/dags/gastos_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/pipelines")

from ingestion.ingestion import exec_ingestion
from transformation.transformation import exec_transformation
from transformation.silver_to_postgres import carregar_para_postgres

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="public_spend_pipeline",
    default_args=default_args,
    description="Pipeline completo de gastos públicos",
    schedule_interval="0 6 * * *",   # roda todo dia às 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,                    # não reprocessa datas passadas
    tags=["spend", "prod"]
) as dag:

    task_ingestion = PythonOperator(
        task_id="download_dados",
        python_callable=exec_ingestion,
    )

    task_transformation = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=exec_transformation,
    )

    task_postgres = PythonOperator(
        task_id="silver_to_postgres",
        python_callable=carregar_para_postgres,
    )

    # Define a ordem de execução
    # Se qualquer task falhar, as seguintes não rodam
    task_ingestion >> task_transformation >> task_postgres