from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJECT_DIR = "/opt/project"

with DAG(
    dag_id="saas_analytics_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["saas", "analytics", "dbt"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"cd {PROJECT_DIR} && python generators/saas-generator.py"
    )

    ingest_raw = BashOperator(
        task_id="ingest_raw_events",
        bash_command=f"cd {PROJECT_DIR} && python ingestion/load_raw.py"
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {PROJECT_DIR}/postgres_proj && dbt run --select staging"
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {PROJECT_DIR}/postgres_proj && dbt run --select marts"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_DIR}/postgres_proj && dbt test"
    )

    generate_data >> ingest_raw >> dbt_run_staging >> dbt_run_marts >> dbt_test