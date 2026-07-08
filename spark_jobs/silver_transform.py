import argparse
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from pipeline_log import log_run, now_utc

EVENT_KEYS = ["timestamp", "visitorid", "event", "itemid", "transactionid"]


def dedupe_events(events):
    # the raw events.csv ships exact duplicate rows; drop them on the natural key
    return events.dropDuplicates(EVENT_KEYS)


def point_in_time_category(events, item_props):
    # Attach the item's category *as it was at the moment of the event*.
    #
    # item_properties is slowly-changing: each item has many categoryid snapshots
    # over time. A plain join + groupBy(max) would pick the newest snapshot, which
    # for an early event is a value from the future -> label/feature leakage.
    #
    # Instead: union the event rows with the category snapshots on one timeline per
    # item, order by timestamp, and carry the last known categoryid forward with
    # last(ignorenulls). An event only ever sees snapshots at or before its own ts.
    cats = (item_props
            .filter(F.col("property") == "categoryid")
            .select("itemid",
                    F.col("timestamp").alias("ts"),
                    F.lit(0).alias("is_event"),
                    F.col("value").cast("long").alias("categoryid")))

    ev = (events
          .withColumn("ts", F.col("timestamp"))
          .withColumn("is_event", F.lit(1))
          .withColumn("categoryid", F.lit(None).cast("long")))

    timeline = ev.unionByName(cats, allowMissingColumns=True)

    # is_event as secondary sort so a snapshot stamped at the exact event ts sorts
    # before the event -> "at or before" includes equal timestamps.
    w = (Window.partitionBy("itemid")
         .orderBy("ts", "is_event")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    enriched = (timeline
                .withColumn("categoryid", F.last("categoryid", ignorenulls=True).over(w))
                .filter(F.col("is_event") == 1)
                .select(*events.columns, "categoryid"))

    # TODO: same pattern could enrich `available` (in-stock at event time) if needed
    return enriched


def run(spark, bronze_dir, out_dir):
    events = spark.read.parquet(f"{bronze_dir}/events")
    props = spark.read.parquet(f"{bronze_dir}/item_properties")

    events = dedupe_events(events)
    enriched = point_in_time_category(events, props)

    enriched.write.mode("overwrite").partitionBy("event_date") \
        .parquet(f"{out_dir}/events_enriched")

    return spark.read.parquet(f"{out_dir}/events_enriched").count()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-dir", default="data/bronze")
    ap.add_argument("--out-dir", default="data/silver")
    ap.add_argument("--no-log", action="store_true", help="skip writing to pipeline_runs")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("silver_transform").getOrCreate()

    started = now_utc()
    t0 = time.perf_counter()
    try:
        rows = run(spark, args.bronze_dir, args.out_dir)
    except Exception as e:
        if not args.no_log:
            log_run("silver_transform", None, time.perf_counter() - t0,
                    "failed", started, str(e))
        raise
    finally:
        spark.stop()

    duration = time.perf_counter() - t0
    print(f"silver_transform wrote {rows} enriched event rows in {duration:.1f}s")
    if not args.no_log:
        log_run("silver_transform", rows, duration, "success", started)


if __name__ == "__main__":
    main()
