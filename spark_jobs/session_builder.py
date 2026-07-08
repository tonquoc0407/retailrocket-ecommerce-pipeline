import argparse
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from pipeline_log import log_run, now_utc

SESSION_TIMEOUT = 1800  # seconds; 30 min of inactivity starts a new session

def tag_sessions(events):
    # attach a session_id to every event (reused by feature_gold for co-occurrence)
    w = Window.partitionBy("visitorid").orderBy("timestamp")

    # gap to the previous event for this visitor, in seconds
    gap = (F.col("timestamp") - F.lag("timestamp").over(w)) / 1000

    # first event of a visitor (lag is null) or a gap over the timeout opens a session
    is_new = ((F.lag("timestamp").over(w).isNull()) | (gap > SESSION_TIMEOUT)).cast("int")

    # running sum of the boundary flag = per-visitor session number (no Python loop)
    session_num = F.sum(is_new).over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow))

    return events.withColumn("session_num", session_num) \
        .withColumn("session_id", F.concat_ws("-", F.col("visitorid"), F.col("session_num")))

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

def run(spark, silver_dir, out_dir):
    events = spark.read.parquet(f"{silver_dir}/events_enriched")
    sessions = build_sessions(events)
    sessions.write.mode("overwrite").partitionBy("session_date") \
        .parquet(f"{out_dir}/sessions")
    return spark.read.parquet(f"{out_dir}/sessions").count()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="data/silver")
    ap.add_argument("--out-dir", default="data/silver")
    ap.add_argument("--no-log", action="store_true", help="skip writing to pipeline_runs")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("session_builder").getOrCreate()

    started = now_utc()
    t0 = time.perf_counter()
    try:
        rows = run(spark, args.silver_dir, args.out_dir)
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
