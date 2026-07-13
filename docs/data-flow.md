# Data flow

How data moves through the layers, from raw CSV to the tables the API serves.

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

By default the job overwrites the whole `events` dataset. Passing `--since` and `--until`
switches it to an incremental load over the half-open window `[since, until)`: the events in
that window are written with dynamic partition overwrite, so only those `event_date`
partitions are replaced and the rest of the history is left in place. A daily run then
rewrites one day instead of rescanning everything, and because the window is half-open,
consecutive days never overlap and re-running a day replaces it rather than doubling it.

The item and category snapshots don't change day to day and rescanning 20M property rows
every morning would defeat the point, so an incremental run skips them — except when they
aren't on disk at all, which it takes as the bootstrap and lands them once. That keeps the
first run of a backfill from leaving Silver to read a path that was never written.

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

`--since` / `--until` work here too, and notably Silver needs **no lookback**: the as-of
join carries a category forward from the item's property *snapshots*, never from
neighbouring events, so an event still sees every snapshot at or before it as long as
`item_properties` is read whole. Duplicates share a timestamp, hence an `event_date`, so
they land in the same window and the dedupe stays complete as well. Sessions are the stage
where that stops being true.

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
4. The opening event's timestamp is carried forward across the session with
   `last(..., ignorenulls=True)` — every event lands in the right session without iterating.

Events are then grouped per session into one row each:

| column | meaning |
|---|---|
| `session_id` | `visitorid-<first timestamp of the session>` |
| `visitorid` | the visitor |
| `start_time` / `end_time` | first / last event timestamp in the session |
| `event_count` | events in the session |
| `has_purchase` | whether any event was a `transaction` |

`has_purchase` is what the abandonment model later keys off (a session with an `addtocart`
but no purchase is the abandonment case).

### Sessions are the stage that can't just read its own window

Bronze and Silver slice cleanly by day. Sessions don't, for two reasons.

**A session is defined by the gap to the *previous* event.** Read a day cold and every
visitor's first event of that day has no predecessor, so it looks like the start of a new
session — the pipeline would silently shred one session into two at every midnight. So an
incremental run reads a day of context *before* the window it emits.

**A session that starts at 23:50 isn't finished at midnight.** The run for that day can only
see the events ingested so far, so it writes the session short. The fix is a lookback: a run
also re-emits `REPROCESS_DAYS = 1` of days already on disk, recomputed from their full set of
events, and dynamic partition overwrite replaces those `session_date` partitions wholesale.
The straddling session is repaired by the next day's run. One day is enough — with a 30-minute
timeout a session cannot span two. This is the same late-arriving-data pattern as
`fct_funnel`'s lookback below, just at the Spark layer.

That repair only works because **`session_id` is keyed on the session's first timestamp**,
not on a running count. A count restarts at 1 inside whatever window the job happens to read,
so the same session would get a different id depending on how much history was loaded — and
day 2's first session would collide with day 1's. Keyed on the start timestamp, a rebuilt
session keeps its id, and the overwrite is an update rather than a duplicate.

`tests/test_incremental.py` pins all of this down, including the assertion that matters most:
running the pipeline day by day lands exactly what a single full pass over the same data does.

## Gold (Spark load + dbt)

The Gold layer lives in Postgres and is modeled with dbt. Work is split between Spark and
dbt on purpose:

**Spark (`spark_jobs/feature_gold.py`)** lands the source tables dbt reads, via JDBC:

- `raw_events`, `raw_sessions`, `raw_category_tree` — copies of the silver/bronze data
- `item_latest` — the *newest* categoryid snapshot per item (row_number over descending
  time). This is deliberately the opposite of Silver's point-in-time join: `dim_items`
  wants an item's current category, not its category at some past event.
- `cooccur_pairs` — item pairs that co-occur in the same session, split into `view` and
  `purchase` signals. This is an O(n²) self-join per session, which is why it runs in Spark
  and not in Postgres SQL. Pairs below `MIN_SUPPORT` are dropped to keep the table small.

Only the first two are facts of a given day. `item_latest` is each item's newest snapshot and
`cooccur_pairs` weights every item pair by how often it was seen across *all* sessions — both
are functions of the whole history, and recomputing them from one day's events is meaningless.
So Gold has two modes: with `--since`/`--until` it lands only `raw_events` and `raw_sessions`
for the window (Postgres has no partitions, so "replace this window" is a delete over the date
range followed by an append), and with no window it does a full rebuild of all five tables.
The daily DAG uses the first; the refresh DAG uses the second. `raw_events` is re-tagged using
the same lookback Sessions uses, so an event just after midnight carries the `session_id` of
the session that opened the night before and still joins to `raw_sessions`.

**dbt (`dbt/retailrocket/`)** does the declarative modeling and testing. Staging views
rename/trim the raw tables; marts build the business tables:

| mart | grain | notes |
|---|---|---|
| `fct_sessions` | session | adds `duration_seconds` |
| `fct_funnel` | category × day | view→cart→purchase counts + step conversion rates; null-category events dropped (can't attribute); **incremental** |
| `dim_categories` | category | `parentid`, `is_root` |
| `dim_items` | item | latest category joined to its parent |
| `feature_table` | item | popularity + item/category conversion — feeds both models |
| `feature_cooccur` | item pair | co-view/co-purchase weights for the recommender |

`feature_table` is item-grained; the co-occurrence pairs are a different grain so they get
their own table rather than being forced into item rows.

`fct_funnel` is materialized `incremental` (`delete+insert` on `categoryid, event_date`).
On a normal run it only recomputes the last few days of the calendar — `is_incremental()`
adds a `event_date >= max(event_date) - 3 days` filter — and replaces those rows rather than
appending, since it's an aggregate and a re-touched day would otherwise double count. The
3-day lookback absorbs late-arriving events that land on an earlier date. The item-grained
`feature_table` aggregates over all of history, so it stays a full-refresh table; only the
day-grained fact is worth the incremental machinery.

dbt tests cover `not_null`/`unique` on every primary key and `relationships` from
`fct_funnel` and `dim_items` back to `dim_categories`.

