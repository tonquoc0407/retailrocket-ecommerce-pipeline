-- run-level history for every spark/dbt job, feeds the dashboard health tab
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           BIGSERIAL PRIMARY KEY,
    task_name        TEXT        NOT NULL,
    rows_processed   BIGINT,
    duration_seconds DOUBLE PRECISION,
    status           TEXT        NOT NULL,   -- success | failed
    started_at       TIMESTAMPTZ NOT NULL,
    error_message    TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_task ON pipeline_runs (task_name, started_at DESC);
