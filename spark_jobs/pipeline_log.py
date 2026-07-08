import os
from datetime import datetime, timezone


def _conn():
    import psycopg2  # imported lazily so transform code runs without the DB driver
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "retailrocket"),
        user=os.getenv("POSTGRES_USER", "retail"),
        password=os.getenv("POSTGRES_PASSWORD", "retail"),
    )


def log_run(task_name, rows_processed, duration_seconds, status,
            started_at, error_message=None):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (task_name, rows_processed, duration_seconds,
                     status, started_at, error_message)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (task_name, rows_processed, duration_seconds,
                 status, started_at, error_message),
            )


def now_utc():
    return datetime.now(timezone.utc)
