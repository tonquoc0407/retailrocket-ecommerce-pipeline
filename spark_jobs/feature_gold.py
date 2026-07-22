import argparse
import os
import time
from datetime import date, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from pipeline_log import connect, log_run, now_utc
from session_builder import REPROCESS_DAYS, tag_sessions

# pairs seen fewer than this many times are dropped -- noise, and keeps the table small.
# at full scale you'd also cap to top-k neighbours per item; fine to skip at this scope.
MIN_SUPPORT = 2

PG_JDBC_VERSION = "org.postgresql:postgresql:42.7.3"

def latest_item_snapshot(item_props):
    # dim_items wants the item's *current* category, so take the newest snapshot per item
    # (this is the opposite of silver's point-in-time join, and that's intentional)
    cats = item_props.filter(F.col("property") == "categoryid")
    w = Window.partitionBy("itemid").orderBy(F.col("timestamp").desc())
    return (cats.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1")
            .select("itemid", F.col("value").cast("long").alias("categoryid")))

def cooccurrence_pairs(events, min_support=MIN_SUPPORT):
    tagged = tag_sessions(events)

    def pairs_for(df, pair_type):
        items = df.select("session_id", "itemid").distinct()
        a = items.withColumnRenamed("itemid", "item_a")
        b = items.withColumnRenamed("itemid", "item_b")
        # item_a < item_b keeps each unordered pair once and drops self-pairs
        return (a.join(b, "session_id")
                .filter(F.col("item_a") < F.col("item_b"))
                .groupBy("item_a", "item_b")
                .agg(F.count("*").alias("weight"))
                .withColumn("pair_type", F.lit(pair_type)))

    views = pairs_for(tagged.filter(F.col("event") == "view"), "view")
    purchases = pairs_for(tagged.filter(F.col("event") == "transaction"), "purchase")
    return views.unionByName(purchases).filter(F.col("weight") >= min_support)

def jdbc_url():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "retailrocket")
    return f"jdbc:postgresql://{host}:{port}/{db}"

def jdbc_props():
    return {
        "user": os.getenv("POSTGRES_USER", "retail"),
        "password": os.getenv("POSTGRES_PASSWORD", "retail"),
        "driver": "org.postgresql.Driver",
    }

def with_load_ts(df):
    # ingestion timestamp, stamped at write time. the event data is a static 2015 export,
    # so freshness measured against the *event* date is meaningless (always months stale).
    # this instead measures pipeline freshness -- did the load actually run recently -- and
    # is what dbt source freshness keys off in sources.yml.
    return df.withColumn("_loaded_at", F.current_timestamp())

def write_table(df, table, url, props):
    # truncate instead of drop-and-recreate so dbt views built on these source tables
    # survive a re-run. (a column change still needs a manual drop / dbt clean.)
    df.write.mode("overwrite").option("truncate", "true").jdbc(url, table, properties=props)

def delete_window(table, date_col, since, until):
    # postgres holds no partitions, so "replace this window" is a delete + append.
    # skipped when the table isn't there yet -- the first incremental run creates it.
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("select to_regclass(%s)", (table,))
            if cur.fetchone()[0] is None:
                return
            cur.execute(
                f"delete from {table} where {date_col} >= %s and {date_col} < %s",
                (since, until),
            )

def run_incremental(spark, silver_dir, url, props, since, until):
    # read events with the same lookback session_builder uses, so an event just after
    # midnight is tagged with the session that opened the night before and its
    # session_id matches raw_sessions -- the dbt session mart joins the two on it.
    emit_from = date.fromisoformat(since) - timedelta(days=REPROCESS_DAYS)
    read_from = emit_from - timedelta(days=1)

    events = (spark.read.parquet(f"{silver_dir}/events_enriched")
              .filter(F.col("event_date") >= F.to_date(F.lit(str(read_from))))
              .filter(F.col("event_date") < F.to_date(F.lit(until))))

    window_events = (tag_sessions(events)
                     .filter(F.col("event_date") >= F.to_date(F.lit(since)))
                     .select("visitorid", "event", "itemid", "categoryid",
                             "event_date", "session_id"))

    # session_builder rebuilds REPROCESS_DAYS of days already on disk (a session that
    # straddles midnight only completes on the next run), so postgres has to replace the
    # same span. session_id is keyed on the session's first timestamp, so a rebuilt
    # session keeps its id: these rows are an update, not a duplicate insert.
    sessions = (spark.read.parquet(f"{silver_dir}/sessions")
                .filter(F.col("session_date") >= F.to_date(F.lit(str(emit_from))))
                .filter(F.col("session_date") < F.to_date(F.lit(until))))

    delete_window("raw_events", "event_date", since, until)
    with_load_ts(window_events).write.mode("append").jdbc(url, "raw_events", properties=props)

    delete_window("raw_sessions", "session_date", str(emit_from), until)
    with_load_ts(sessions).write.mode("append").jdbc(url, "raw_sessions", properties=props)

    return sessions.count()

def run(spark, silver_dir, bronze_dir, url, props, since=None, until=None):
    if since and until:
        return run_incremental(spark, silver_dir, url, props, since, until)

    events = spark.read.parquet(f"{silver_dir}/events_enriched")
    sessions = spark.read.parquet(f"{silver_dir}/sessions")
    categories = spark.read.parquet(f"{bronze_dir}/category_tree")
    item_props = spark.read.parquet(f"{bronze_dir}/item_properties")

    # land the tables dbt reads as sources. events carry session_id so the dbt
    # session-feature mart (abandonment model) can aggregate per session.
    tagged = tag_sessions(events)
    write_table(with_load_ts(tagged.select("visitorid", "event", "itemid", "categoryid",
                                           "event_date", "session_id")),
                "raw_events", url, props)
    write_table(with_load_ts(sessions), "raw_sessions", url, props)

    # the other three are whole-history aggregates, not facts of a single day:
    # cooccur_pairs counts co-views across every session there has ever been, and
    # item_latest is the newest snapshot per item. neither can be sliced by date, so
    # they're rebuilt on a full run -- the bootstrap load and the weekly refresh dag --
    # and left alone by the daily one. what they read is static anyway: bronze's
    # incremental path never rewrites item_properties or the category tree.
    write_table(categories, "raw_category_tree", url, props)
    write_table(latest_item_snapshot(item_props), "item_latest", url, props)
    write_table(cooccurrence_pairs(events), "cooccur_pairs", url, props)

    return sessions.count()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="data/silver")
    ap.add_argument("--bronze-dir", default="data/bronze")
    ap.add_argument("--since", help="YYYY-MM-DD, inclusive; land this window's facts only")
    ap.add_argument("--until", help="YYYY-MM-DD, exclusive; land this window's facts only")
    ap.add_argument("--no-log", action="store_true", help="skip writing to pipeline_runs")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("feature_gold")
             .config("spark.jars.packages", PG_JDBC_VERSION)
             .getOrCreate())

    url, props = jdbc_url(), jdbc_props()

    started = now_utc()
    t0 = time.perf_counter()
    try:
        rows = run(spark, args.silver_dir, args.bronze_dir, url, props,
                   args.since, args.until)
    except Exception as e:
        if not args.no_log:
            log_run("feature_gold", None, time.perf_counter() - t0,
                    "failed", started, str(e))
        raise
    finally:
        spark.stop()

    duration = time.perf_counter() - t0
    print(f"feature_gold landed gold source tables in {duration:.1f}s")
    if not args.no_log:
        log_run("feature_gold", rows, duration, "success", started)

if __name__ == "__main__":
    main()
