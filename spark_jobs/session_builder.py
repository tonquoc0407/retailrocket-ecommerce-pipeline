import argparse
import time
from datetime import date, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from pipeline_log import log_run, now_utc

SESSION_TIMEOUT = 1800  # seconds; 30 min of inactivity starts a new session

# how many already-written session_date partitions an incremental run rebuilds. a session
# that straddles midnight is truncated by the run that first sees it -- the rest of its
# events only arrive with the next day -- so the next run recomputes that day in full and
# overwrites it. one day is enough: with a 30-minute timeout a session cannot span two.
REPROCESS_DAYS = 1

def tag_sessions(events):
    # attach a session_id to every event (reused by feature_gold for co-occurrence)
    w = Window.partitionBy("visitorid").orderBy("timestamp")

    # gap to the previous event for this visitor, in seconds
    prev = F.lag("timestamp").over(w)
    gap = (F.col("timestamp") - prev) / 1000

    # first event of a visitor (lag is null) or a gap over the timeout opens a session.
    # materialised as a column because the next window can't nest a window inside it.
    tagged = events.withColumn(
        "is_session_start", (prev.isNull() | (gap > SESSION_TIMEOUT)).cast("int")
    )

    # carry the opening event's timestamp forward across the session and key the id on
    # it. a running count would be simpler but is only stable if the job reads the whole
    # history: incremental runs read a window, and the count would restart at 1 inside
    # it, so the same session would get a different id (and collide with another
    # visitor's) depending on how much data happened to be loaded.
    running = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    start_ms = F.last(
        F.when(F.col("is_session_start") == 1, F.col("timestamp")), ignorenulls=True
    ).over(running)

    return (tagged
            .withColumn("session_start_ms", start_ms)
            .drop("is_session_start")
            .withColumn("session_id",
                        F.concat_ws("-", F.col("visitorid"), F.col("session_start_ms"))))

def build_sessions(events):
    tagged = tag_sessions(events)
    is_purchase = F.when(F.col("event") == "transaction", 1).otherwise(0)

    sessions = (tagged.groupBy("session_id", "visitorid")
                .agg(F.min("timestamp").alias("start_ms"),
                     F.max("timestamp").alias("end_ms"),
                     F.count("*").alias("event_count"),
                     (F.max(is_purchase) == 1).alias("has_purchase")))

    return (sessions
            .withColumn("start_time", F.to_timestamp(F.col("start_ms") / 1000))
            .withColumn("end_time", F.to_timestamp(F.col("end_ms") / 1000))
            .withColumn("session_date", F.to_date(F.col("start_time")))
            .select("session_id", "visitorid", "start_time", "end_time",
                    "event_count", "has_purchase", "session_date"))

def run(spark, silver_dir, out_dir, since=None, until=None):
    events = spark.read.parquet(f"{silver_dir}/events_enriched")

    if not (since or until):
        sessions = build_sessions(events)
        sessions.write.mode("overwrite").partitionBy("session_date") \
            .parquet(f"{out_dir}/sessions")
        return spark.read.parquet(f"{out_dir}/sessions").count()

    if not since:
        raise ValueError("--until needs --since: the lookback is measured back from it")

    # sessions are the one stage that can't just read its own window. a session is
    # defined by the gap to the *previous* event, so the first event of the window
    # can't be classified without the tail of the day before it -- read cold and every
    # visitor's first event of the day looks like a new session.
    #
    # so: emit the window plus REPROCESS_DAYS of days already on disk (they may hold
    # midnight-straddling sessions that this window completes), and read one day of
    # context before that to get the gaps right at the emit boundary. the days read for
    # context are computed and thrown away.
    emit_from = date.fromisoformat(since) - timedelta(days=REPROCESS_DAYS)
    read_from = emit_from - timedelta(days=1)

    events = events.filter(F.col("event_date") >= F.to_date(F.lit(str(read_from))))
    if until:
        events = events.filter(F.col("event_date") < F.to_date(F.lit(until)))

    sessions = build_sessions(events).filter(
        F.col("session_date") >= F.to_date(F.lit(str(emit_from)))
    )

    # every session_date we touch is rewritten from its full set of events, so the
    # overwrite replaces those partitions wholesale rather than appending to them.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sessions.write.mode("overwrite").partitionBy("session_date") \
        .parquet(f"{out_dir}/sessions")
    return sessions.count()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="data/silver")
    ap.add_argument("--out-dir", default="data/silver")
    ap.add_argument("--since", help="YYYY-MM-DD, inclusive; rebuild sessions from this date")
    ap.add_argument("--until", help="YYYY-MM-DD, exclusive; rebuild sessions before this date")
    ap.add_argument("--no-log", action="store_true", help="skip writing to pipeline_runs")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("session_builder").getOrCreate()

    started = now_utc()
    t0 = time.perf_counter()
    try:
        rows = run(spark, args.silver_dir, args.out_dir, args.since, args.until)
    except Exception as e:
        if not args.no_log:
            log_run("session_builder", None, time.perf_counter() - t0,
                    "failed", started, str(e))
        raise
    finally:
        spark.stop()

    duration = time.perf_counter() - t0
    print(f"session_builder wrote {rows} sessions in {duration:.1f}s")
    if not args.no_log:
        log_run("session_builder", rows, duration, "success", started)

if __name__ == "__main__":
    main()
