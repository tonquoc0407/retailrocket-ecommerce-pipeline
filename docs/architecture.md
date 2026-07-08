# Architecture

End-to-end view of the platform. The full diagram is in
[`architecture.mermaid`](../architecture.mermaid); this doc explains the pieces and why each
technology was chosen.

## Overview

A daily batch pipeline over the RetailRocket dataset following a medallion layout:

```
raw CSV -> Bronze (Parquet) -> Silver (clean + point-in-time) -> Sessions
        -> Gold (Postgres, dbt) -> models (recommender + abandonment)
        -> FastAPI -> React dashboard
```

Airflow orchestrates it; Prometheus + Grafana and a `pipeline_runs` table watch it.

## Layers

- **Bronze** (`spark_jobs/bronze_ingest.py`) — raw CSVs to Parquet, events partitioned by
  date. No logic beyond typing and partitioning, so a bad transform downstream can always be
  replayed from Bronze.
- **Silver** (`spark_jobs/silver_transform.py`) — dedupe, then the point-in-time join that
  attaches each event the item's category **as of that event's timestamp**, never the latest
  one. This is the core engineering piece; using the latest snapshot would leak future
  information into training features. See `docs/data-flow.md`.
- **Sessions** (`spark_jobs/session_builder.py`) — visitor events grouped into sessions with a
  30-minute inactivity timeout, computed with a window function (not a Python loop).
- **Gold** (`spark_jobs/feature_gold.py` + `dbt/`) — Spark lands raw/aggregate tables in
  Postgres; dbt builds the marts (`fct_funnel`, `fct_sessions`, `dim_*`, `feature_table`,
  `feature_sessions`, `feature_cooccur`) and runs `not_null`/`unique`/`relationships` tests.
- **Models** (`ml/`) — a recommender (ALS baseline, item2vec alternative) and a cart-
  abandonment classifier (XGBoost baseline, algorithm chosen in `config.yaml`).
- **Serving** (`api/`) — FastAPI reads the gold tables and the trained model.
- **Dashboard** (`dashboard/`) — React + Recharts: funnel, top items, recommend demo, health.

## Why these choices

| Choice | Reason |
|---|---|
| PySpark | The point-in-time join is over ~20M item-property rows; window functions handle it and the same code scales beyond one machine. |
| Parquet for Bronze/Silver | Columnar + partitioned by date = cheap reprocessing; no DB needed for intermediate data. |
| Postgres for Gold | Marts are small and queried by the API with plain SQL; a warehouse would be overkill at this scale. |
| dbt | Declarative marts with built-in tests and lineage, instead of hand-written load scripts. |
| ALS / XGBoost as baselines | Both are strong, well-understood defaults; the training scripts keep the interface open so alternatives swap in without touching the API. |
| FastAPI | Pydantic validation + automatic OpenAPI, and `/metrics` via one instrumentator line. |
| Airflow | Daily batch with retries, a failure webhook, and statsd metrics out of the box. |
| Prometheus + Grafana | Standard pairing; the API already exposes Prometheus metrics, Airflow exports via statsd. |
| Docker Compose | Single-host dev and deploy target — no Kubernetes or cloud-specific setup in scope. |

## Data leakage guards

Two deliberate ones, both documented in code:

1. **Point-in-time join** — category as of event time, in Silver.
2. **Time-based train/test split** for the abandonment model — train on earlier sessions,
   test on later ones, never a random split.
