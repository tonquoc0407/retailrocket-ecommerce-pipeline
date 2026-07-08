# Data flow

How data moves through the layers. Written incrementally, one section per phase.

## Bronze

Raw CSVs land in `data/raw/` (downloaded manually from Kaggle):

- `events.csv` — `timestamp, visitorid, event, itemid, transactionid`
- `item_properties_part1.csv` + `part2.csv` — `timestamp, itemid, property, value`
- `category_tree.csv` — `categoryid, parentid`

`spark_jobs/bronze_ingest.py` reads each with an explicit schema (no `inferSchema`,
which would waste a full scan of the 20M-row properties file) and writes Parquet to
`data/bronze/` with no cleaning yet — Bronze is a faithful copy of the source.

- `events` is partitioned by `event_date`, derived from the epoch-millisecond
  `timestamp` (`from_unixtime(timestamp/1000)` → `to_date`). Partitioning by day keeps
  later day-scoped reads (funnel-by-day, time-based train/test split) cheap.
- `item_properties` part1 and part2 are unioned into one table. It stays un-partitioned
  here; the point-in-time logic that actually needs the timestamps happens in Silver.
- `category_tree` is copied as-is.

Each run writes one row to `pipeline_runs` (task, rows written, duration, status) so the
dashboard health tab has a record of it. On failure the job logs a `failed` row and
re-raises.
