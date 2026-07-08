import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (LongType, StringType, StructField, StructType)

from pipeline_log import log_run, now_utc

# Raw schemas are fixed and small, so declare them instead of inferSchema
# (avoids an extra full scan of the 20M-row properties file).
EVENTS_SCHEMA = StructType([
    StructField("timestamp", LongType()),      # epoch millis
    StructField("visitorid", LongType()),
    StructField("event", StringType()),
    StructField("itemid", LongType()),
    StructField("transactionid", LongType()),  # null unless event == transaction
])

ITEM_PROPS_SCHEMA = StructType([
    StructField("timestamp", LongType()),
    StructField("itemid", LongType()),
    StructField("property", StringType()),
    StructField("value", StringType()),
])

CATEGORY_SCHEMA = StructType([
    StructField("categoryid", LongType()),
    StructField("parentid", LongType()),
])

def read_csv(spark, path, schema):
    return spark.read.csv(path, header=True, schema=schema)

def run(spark, raw_dir, out_dir):
    events = read_csv(spark, f"{raw_dir}/events.csv", EVENTS_SCHEMA)
    # partition column derived from the epoch-ms timestamp
    events = events.withColumn(
        "event_date",
        F.to_date(F.from_unixtime(F.col("timestamp") / 1000)),
    )

    props = read_csv(spark, f"{raw_dir}/item_properties_part1.csv", ITEM_PROPS_SCHEMA) \
        .unionByName(read_csv(spark, f"{raw_dir}/item_properties_part2.csv", ITEM_PROPS_SCHEMA))

    categories = read_csv(spark, f"{raw_dir}/category_tree.csv", CATEGORY_SCHEMA)

    events.write.mode("overwrite").partitionBy("event_date") \
        .parquet(f"{out_dir}/events")
    props.write.mode("overwrite").parquet(f"{out_dir}/item_properties")
    categories.write.mode("overwrite").parquet(f"{out_dir}/category_tree")

    events_written = spark.read.parquet(f"{out_dir}/events").count()
    return events_written

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--out-dir", default="data/bronze")
    ap.add_argument("--no-log", action="store_true", help="skip writing to pipeline_runs")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bronze_ingest").getOrCreate()

    started = now_utc()
    t0 = time.perf_counter()
    try:
        rows = run(spark, args.raw_dir, args.out_dir)
    except Exception as e:
        if not args.no_log:
            log_run("bronze_ingest", None, time.perf_counter() - t0,
                    "failed", started, str(e))
        raise
    finally:
        spark.stop()

    duration = time.perf_counter() - t0
    print(f"bronze_ingest wrote {rows} event rows in {duration:.1f}s")
    if not args.no_log:
        log_run("bronze_ingest", rows, duration, "success", started)

if __name__ == "__main__":
    main()
