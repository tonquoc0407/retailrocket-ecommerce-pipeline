import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerts import notify_failure

# where the repo is mounted inside the Airflow containers (see docker-compose)
PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/project")
DBT_DIR = f"{PROJECT_HOME}/dbt/retailrocket"

default_args = {
    "owner": "retailrocket",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}

def run_py(script, args=""):
    return f"cd {PROJECT_HOME} && python {script} {args}".strip()

with DAG(
    dag_id="retailrocket_pipeline",
    description="bronze -> silver -> sessions -> gold -> dbt -> models",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["retailrocket"],
) as dag:
    bronze = BashOperator(task_id="bronze_ingest", bash_command=run_py("spark_jobs/bronze_ingest.py"))
    silver = BashOperator(task_id="silver_transform", bash_command=run_py("spark_jobs/silver_transform.py"))
    sessions = BashOperator(task_id="session_builder", bash_command=run_py("spark_jobs/session_builder.py"))
    gold = BashOperator(task_id="feature_gold", bash_command=run_py("spark_jobs/feature_gold.py"))

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    train_als = BashOperator(task_id="train_als", bash_command=run_py("ml/recommender/train_als.py"))
    train_item2vec = BashOperator(task_id="train_item2vec", bash_command=run_py("ml/recommender/train_item2vec.py"))
    train_abandon = BashOperator(task_id="train_abandonment", bash_command=run_py("ml/abandonment/train.py", "--all"))

    bronze >> silver >> sessions >> gold >> dbt_run >> dbt_test
    dbt_test >> [train_als, train_item2vec, train_abandon]
