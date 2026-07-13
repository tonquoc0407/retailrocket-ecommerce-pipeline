import os
from datetime import timedelta

from alerts import notify_failure

# where the repo is mounted inside the Airflow containers (see docker-compose)
PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/project")
DBT_DIR = f"{PROJECT_HOME}/dbt/retailrocket"

# half-open [start, end) so each run owns exactly one day and reruns are idempotent.
# not an f-string: the braces have to survive into the bash_command for airflow to
# render them as jinja.
DATE_WINDOW = "--since {{ data_interval_start | ds }} --until {{ data_interval_end | ds }}"

default_args = {
    "owner": "retailrocket",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}

def run_py(script, args=""):
    return f"cd {PROJECT_HOME} && python {script} {args}".strip()

def dbt(command):
    return f"dbt {command} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
