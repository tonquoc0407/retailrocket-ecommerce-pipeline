# Monitoring

Three layers: structured logs, a `pipeline_runs` table for job history, and
Prometheus + Grafana for live metrics.

## Structured logging

`api/logging_conf.py` sets up a JSON formatter on the root logger. Every FastAPI request is
logged as one line by the middleware in `api/main.py`:

```
{"ts": "...", "level": "INFO", "logger": "api", "msg": "request",
 "method": "GET", "path": "/top-items", "status": 200, "latency_ms": 7.9}
```

JSON from day one means a log shipper (Filebeat/Fluentd) can be attached later without
changing the app. Spark jobs keep it minimal — they just write a row to `pipeline_runs`.

## pipeline_runs table

Written by `spark_jobs/pipeline_log.py` at the end of each job (`db/init.sql` for the schema):

| column | meaning |
|---|---|
| run_id | serial PK |
| task_name | `bronze_ingest`, `silver_transform`, ... |
| rows_processed | rows written by the job |
| duration_seconds | wall-clock time |
| status | `success` / `failed` |
| started_at | job start (UTC) |
| error_message | set on failure |

`GET /pipeline-health` returns the latest run per task, which feeds the dashboard's
Pipeline health tab.

## Prometheus

`monitoring/prometheus.yml` scrapes two targets:

- **fastapi** — `api:8000/metrics` (via `prometheus-fastapi-instrumentator`). Key series:
  `http_requests_total`, `http_request_duration_seconds_bucket`.
- **airflow** — Airflow emits statsd; `statsd-exporter` (mapping in
  `monitoring/statsd_mapping.yml`) converts it to Prometheus. Key series:
  `airflow_task_duration_ms`, `airflow_task_failures_total`, `airflow_dagrun_duration_*`.

## Grafana

Datasource and dashboard are provisioned from `monitoring/grafana/provisioning/`; the
dashboard JSON is `monitoring/grafana/dashboards/retailrocket.json`. Panels:

| panel | query |
|---|---|
| API request rate | `sum(rate(http_requests_total[5m]))` |
| API latency p95 | `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))` |
| API 5xx error rate | `sum(rate(http_requests_total{status=~"5.."}[5m]))` |
| Airflow task failures (1h) | `sum(increase(airflow_task_failures_total[1h]))` |
| Airflow task duration | `airflow_task_duration_ms` |

Open Grafana at `http://localhost:3000` (admin/admin by default), dashboard
**RetailRocket Platform**. During a demo: run the DAG, watch task duration/failure panels
move, then hit the API and watch request-rate and latency respond.

## Failure alerts

`dags/alerts.py` posts a short message to `ALERT_WEBHOOK_URL` (Slack/Discord) via the DAG's
`on_failure_callback`. Unset webhook = no-op, so local runs don't error.
