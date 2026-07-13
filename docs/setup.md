# Setup

Two ways to run: piece by piece for development (the sections up to Tests), or the whole
stack at once with Docker Compose (last section).

## Requirements

- Python 3.10+ (`pip install -r requirements.txt`)
- **JDK 17** for Spark. Spark 3.5 does not run on JDK 21+/25 (the Security Manager was
  removed, and bundled Hadoop still calls it). Point Spark at a 17 JDK:
  ```
  export JAVA_HOME=/path/to/jdk-17
  ```
- Docker (for Postgres)

## Postgres

`pipeline_runs` lives in Postgres. Bring it up before running any Spark job that logs:

```
cp .env.example .env        # adjust creds if needed
docker compose up -d postgres
```

`db/init.sql` creates the `pipeline_runs` table on first boot.

## Run the Bronze job

Place the Kaggle CSVs under `data/raw/`, then:

```
export JAVA_HOME=/path/to/jdk-17
python spark_jobs/bronze_ingest.py            # reads data/raw, writes data/bronze
python spark_jobs/bronze_ingest.py --no-log   # skip the pipeline_runs write
```

## Gold layer (Spark load + dbt)

Load the silver/bronze data into Postgres, then build the gold models:

```
export JAVA_HOME=/path/to/jdk-17
python spark_jobs/feature_gold.py             # JDBC load -> raw_* + item_latest + cooccur_pairs

dbt run  --project-dir dbt/retailrocket --profiles-dir dbt/retailrocket
dbt test --project-dir dbt/retailrocket --profiles-dir dbt/retailrocket
```

dbt reads its connection from the same `POSTGRES_*` env vars as everything else
(`dbt/retailrocket/profiles.yml`). Models land in the `gold` schema.

## Models

After the gold layer is loaded (recommenders read silver Parquet; abandonment reads the
`feature_sessions` mart from Postgres):

```
export JAVA_HOME=/path/to/jdk-17

# recommender -> item_recommendations table (method column: als / item2vec)
python ml/recommender/train_als.py
python ml/recommender/train_item2vec.py

# cart-abandonment classifier -> ml/model_registry/abandon_<algo>.pkl
python ml/abandonment/train.py           # algorithm from ml/abandonment/config.yaml
python ml/abandonment/train.py --all     # train + compare xgboost / rf / logistic
```

## API

Serve the gold tables and the trained abandonment model:

```
uvicorn api.main:app --reload                 # http://localhost:8000, /docs for Swagger
```

`/predict-abandon` returns 503 until an abandonment model exists in `ml/model_registry/`;
the other endpoints only need the gold tables loaded. See `docs/api.md` for the routes.

## Dashboard

React + Vite frontend (`dashboard/`). It calls the API via a dev proxy, so start the API
first, then:

```
cd dashboard
npm install
npm run dev                                   # http://localhost:5173
```

## Tests

```
export JAVA_HOME=/path/to/jdk-17
pytest tests/                                 # runs against tests/fixtures, no Postgres needed
```

## Full stack (Docker Compose)

Brings up Postgres, Airflow (webserver + scheduler), the API, Prometheus, Grafana, and the
statsd-exporter on one network:

```
cp .env.example .env                          # adjust creds/webhook if needed
docker compose up -d --build
```

Services:

| service | url | notes |
|---|---|---|
| Airflow | http://localhost:8080 | login admin/admin |
| API | http://localhost:8000 | `/docs`, `/metrics` |
| Prometheus | http://localhost:9090 | scrapes api + airflow |
| Grafana | http://localhost:3000 | admin/admin, dashboard "RetailRocket Platform" |

Seed data and run the pipeline:

1. Put the Kaggle CSVs under `data/raw/` (the repo is mounted into the Airflow containers).
2. In the Airflow UI, unpause **retailrocket_pipeline**. With `catchup=True` it backfills the
   dataset's calendar (2015-05-03 → 2015-09-18) one day at a time:
   bronze → silver → sessions → gold facts → dbt run/test. `max_active_runs=1`, so the 139
   runs go through in order rather than 139 Spark jobs at once.
3. Unpause **retailrocket_refresh** (weekly) for the whole-history work a daily window can't
   express: a full `feature_gold` rebuild (`cooccur_pairs`, `item_latest`) and the three
   trainers.
4. Once they finish, the API endpoints return data and the Grafana panels populate.

The dashboard isn't containerised — run it with `npm run dev` (above) pointing at the API.

Tear down with `docker compose down` (add `-v` to also drop the Postgres volume).
