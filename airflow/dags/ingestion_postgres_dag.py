from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="ingestion_postgres_tmdb",
    description="Lance le script d'ingestion TMDB vers PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule="0 10 * * 1#1",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "moviedb", "postgres"],
) as dag:
    run_ingestion_postgres = BashOperator(
        task_id="run_main_ingestion_postgre",
        bash_command="python /opt/project/src/ingestion/main_ingestion_postgre.py",
        append_env=True,
    )

    run_ingestion_postgres
