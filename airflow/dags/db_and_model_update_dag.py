from datetime import datetime, timedelta
import os
import sys
import importlib

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Rendre l'import ML robuste pour Airflow Docker (/opt/project/src) et local
project_src_paths = [
    "/opt/project/src",
    os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src")),
]
for project_src_path in project_src_paths:
    if project_src_path not in sys.path and os.path.exists(project_src_path):
        sys.path.insert(0, project_src_path)


def run_train_and_save_models(ml_model: str, targets: list[str]):
    ml_module = importlib.import_module("ml.machine_learning_utils")
    return ml_module.train_and_save_models(ml_model=ml_model, targets=targets)

with DAG(
    dag_id="db_and_model_update",
    description="Lance les scripts de mise à jour de la base de données et du modèle de ML",
    start_date=datetime(2026, 1, 1),
    schedule="0 14 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["update", "moviedb", "mlmodel"],
) as dag:
    run_db_update = BashOperator(
        task_id="run_db_update",
        bash_command="python /opt/project/src/ingestion/main_update_postgres.py",
        retries=2,
        retry_delay=timedelta(minutes=2),
        append_env=True,
    )

    lr_model_update = PythonOperator(
        task_id="run_lr_model_update",
        python_callable=run_train_and_save_models,
        op_kwargs={'ml_model': 'lr', 'targets': ['revenue', 'popularity', 'vote_average']},
    )

    rf_model_update = PythonOperator(
        task_id="run_rf_model_update",
        python_callable=run_train_and_save_models,
        op_kwargs={'ml_model': 'rf', 'targets': ['revenue', 'popularity', 'vote_average']},
    )

    xgb_model_update = PythonOperator(
        task_id="run_xgb_model_update",
        python_callable=run_train_and_save_models,
        op_kwargs={'ml_model': 'xgb', 'targets': ['revenue', 'popularity', 'vote_average']},
    )

    run_db_update >> lr_model_update >> rf_model_update >> xgb_model_update