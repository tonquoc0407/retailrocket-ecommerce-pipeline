from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from common import DATE_WINDOW, dbt, default_args, run_py

# the daily pipeline. every stage takes the run's [data_interval_start, data_interval_end)
# and touches only that window's partitions, so a retry or a re-run of one day replaces
# that day instead of doubling it. whole-history work -- co-occurrence weights, the
# latest-snapshot dimension, model training -- can't be sliced by day and lives in
# retailrocket_refresh instead.
with DAG(
    dag_id="retailrocket_pipeline",
    description="daily bronze -> silver -> sessions -> gold facts -> dbt",
    # the dataset spans 2015-05-03..2015-09-18, so the backfill runs on the data's own
    # calendar, not wall-clock time. end_date caps the logical date, which is the start
    # of each interval, so 09-18 still gets its [09-18, 09-19) run.
    start_date=datetime(2015, 5, 3),
    end_date=datetime(2015, 9, 18),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,  # one spark job at a time; the backfill is 139 sequential days
    default_args=default_args,
    tags=["retailrocket"],
) as dag:
    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command=run_py("spark_jobs/bronze_ingest.py", DATE_WINDOW),
    )
    silver = BashOperator(
        task_id="silver_transform",
        bash_command=run_py("spark_jobs/silver_transform.py", DATE_WINDOW),
    )
    # reads a couple of days either side of the window -- a session that crosses midnight
    # is only complete once the next day lands. see session_builder.REPROCESS_DAYS.
    sessions = BashOperator(
        task_id="session_builder",
        bash_command=run_py("spark_jobs/session_builder.py", DATE_WINDOW),
    )
    gold = BashOperator(
        task_id="feature_gold",
        bash_command=run_py("spark_jobs/feature_gold.py", DATE_WINDOW),
    )

    dbt_run = BashOperator(task_id="dbt_run", bash_command=dbt("run"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=dbt("test"))

    bronze >> silver >> sessions >> gold >> dbt_run >> dbt_test
