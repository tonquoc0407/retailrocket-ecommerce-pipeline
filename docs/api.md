# API

FastAPI app (`api/main.py`). Reads the Gold tables from Postgres and the trained
abandonment model from `ml/model_registry/`. Every request is logged as one JSON line
(method, path, status, latency_ms) and `/metrics` is exposed for Prometheus.

Run locally:

```
uvicorn api.main:app --reload
```

## GET /recommend/{item_id}

Top-N related items. Reads the precomputed `item_recommendations` table; if the item has no
trained recommendations it falls back to co-purchase neighbours from `feature_cooccur`.

| param | in | default | notes |
|---|---|---|---|
| item_id | path | — | item to recommend for |
| method | query | `als` | `als` or `item2vec` |
| n | query | 10 | 1–100 |

```
GET /recommend/12345?method=als&n=3
{
  "item_id": 12345, "source": "recommender", "method": "als",
  "items": [
    {"rec_item_id": 92, "score": 0.967, "rank": 1},
    {"rec_item_id": 211, "score": 0.967, "rank": 2},
    {"rec_item_id": 179, "score": 0.964, "rank": 3}
  ]
}
```

`source` is `recommender` or `cooccur_fallback` (cold-start).

## GET /funnel-stats

Funnel counts + conversion rates per category per day, from `fct_funnel`. All filters
optional.

| param | in | notes |
|---|---|---|
| category_id | query | filter to one category |
| from | query | `event_date >=` (ISO date) |
| to | query | `event_date <=` (ISO date) |

```
GET /funnel-stats?category_id=1&from=2015-06-01&to=2015-06-30
[
  {"category_id": 1, "event_date": "2015-06-02", "views": 40, "carts": 12,
   "purchases": 5, "cart_rate": 0.3, "purchase_rate": 0.416}
]
```

## POST /predict-abandon

Abandonment probability for an at-risk (has-cart) session. Features must match the trained
model (see `docs/ml.md`).

```
POST /predict-abandon
{ "start_hour": 14, "event_count": 8, "n_views": 6, "n_carts": 1,
  "n_items": 4, "n_categories": 2, "views_per_item": 1.5 }

{ "abandon_probability": 0.671 }
```

## GET /pipeline-health

Latest run per task from `pipeline_runs` (newest first) — feeds the dashboard health tab.

```
GET /pipeline-health
[
  {"task_name": "bronze_ingest", "status": "success", "rows_processed": 25047,
   "duration_seconds": 6.9, "started_at": "2026-07-08T10:00:00Z", "error_message": null}
]
```

## GET /metrics

Prometheus exposition (`prometheus-fastapi-instrumentator`): request count, latency
histogram, error rate. Scraped per `monitoring/prometheus.yml`.
