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

## Silver

`spark_jobs/silver_transform.py` turns the raw Bronze events into a clean, enriched
event table at `data/silver/events_enriched` (still partitioned by `event_date`).

Two things happen:

**1. Dedupe.** The raw `events.csv` contains exact duplicate rows. They're dropped on the
natural key `(timestamp, visitorid, event, itemid, transactionid)`.

**2. Point-in-time category join.** Each event gets the item's `categoryid` *as it was at
the moment the event happened* — not the item's current/latest category.

This is the most important correctness step in the pipeline. `item_properties` is
slowly-changing: one item has many `categoryid` snapshots over its lifetime. The naive
approach — join events to properties on `itemid`, then keep the max/latest snapshot — is
wrong, because for an event early in time it would attach a category value that was only
set *later*. That future value leaking into a training feature would inflate model metrics
in a way that never holds in production.

The correct version treats it as an as-of join:

1. Put event rows and category-snapshot rows on one timeline per `itemid`.
2. Order by timestamp (snapshots sort before events at an equal timestamp, so "valid at or
   before" includes an exact match).
3. Carry the last non-null `categoryid` forward with a windowed
   `last(..., ignorenulls=True)` over all rows up to and including the current one.

An event therefore only ever sees snapshots at or before its own timestamp. An event that
predates the item's first snapshot gets a null category. (The same pattern could enrich
other time-varying properties like `available` later.)

