# Setup

Grows as phases land. Right now it covers the Bronze layer and its Postgres dependency.

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

## Tests

```
export JAVA_HOME=/path/to/jdk-17
pytest tests/                                 # runs against tests/fixtures, no Postgres needed
```
