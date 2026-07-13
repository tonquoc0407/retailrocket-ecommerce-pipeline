from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from common import dbt, default_args, run_py

# the parts of the platform that a daily window can't express.
#
# cooccur_pairs weights every pair of items by how often they were seen together across
# *all* sessions, and item_latest is each item's newest snapshot -- both are functions of
# the whole history, so recomputing them from one day's events is meaningless. the models
# are the same: ALS and item2vec train on the full interaction matrix.
#
# so feature_gold runs here with no date window (its full-refresh path, which rebuilds
# those three tables), and the three trainers hang off it. weekly is enough -- a
# recommender does not get materially better for being retrained overnight.
with DAG(
    dag_id="retailrocket_refresh",
    description="weekly full rebuild of the history-wide tables, then retrain",
    start_date=datetime(2015, 5, 3),
    schedule="@weekly",
    catchup=False,  # only the newest rebuild matters; skipping missed weeks is fine
    max_active_runs=1,
    default_args=default_args,
    tags=["retailrocket"],
) as dag:
    gold_full = BashOperator(
        task_id="feature_gold_full",
        bash_command=run_py("spark_jobs/feature_gold.py"),
    )
    dbt_run = BashOperator(task_id="dbt_run", bash_command=dbt("run --full-refresh"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=dbt("test"))

    train_als = BashOperator(
        task_id="train_als", bash_command=run_py("ml/recommender/train_als.py")
    )
    train_item2vec = BashOperator(
        task_id="train_item2vec", bash_command=run_py("ml/recommender/train_item2vec.py")
    )
    train_abandon = BashOperator(
        task_id="train_abandonment", bash_command=run_py("ml/abandonment/train.py", "--all")
    )

    gold_full >> dbt_run >> dbt_test >> [train_als, train_item2vec, train_abandon]
