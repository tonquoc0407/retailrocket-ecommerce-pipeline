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

## Sessions

`spark_jobs/session_builder.py` reconstructs browsing sessions from the enriched events and
writes them to `data/silver/sessions` (partitioned by `session_date`).

A session is a run of activity by one visitor with no gap longer than **30 minutes**
(`SESSION_TIMEOUT = 1800s`, the usual web-analytics default). The reconstruction is done
entirely with window functions, no per-visitor Python loop:

1. Partition by `visitorid`, order by `timestamp`.
2. `lag(timestamp)` gives the previous event's time; the gap in seconds is the difference.
3. A new session starts when there's no previous event (first event for the visitor) or the
   gap exceeds the timeout. That boolean is cast to 0/1.
4. A running `sum()` of that flag over the ordered window is the session number — every
   event lands in the right session without iterating.

Events are then grouped per `(visitorid, session_num)` into one row per session:

| column | meaning |
|---|---|
| `session_id` | `visitorid-session_num` |
| `visitorid` | the visitor |
| `start_time` / `end_time` | first / last event timestamp in the session |
| `event_count` | events in the session |
| `has_purchase` | whether any event was a `transaction` |

`has_purchase` is what the abandonment model later keys off (a session with an `addtocart`
but no purchase is the abandonment case).

