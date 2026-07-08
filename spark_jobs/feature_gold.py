import argparse
import os
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from pipeline_log import log_run, now_utc
from session_builder import tag_sessions

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

def write_table(df, table, url, props):
    # truncate instead of drop-and-recreate so dbt views built on these source tables
    # survive a re-run. (a column change still needs a manual drop / dbt clean.)
    df.write.mode("overwrite").option("truncate", "true").jdbc(url, table, properties=props)

def run(spark, silver_dir, bronze_dir, url, props):
    events = spark.read.parquet(f"{silver_dir}/events_enriched")
    sessions = spark.read.parquet(f"{silver_dir}/sessions")
    categories = spark.read.parquet(f"{bronze_dir}/category_tree")
    item_props = spark.read.parquet(f"{bronze_dir}/item_properties")

    # land the tables dbt reads as sources. events carry session_id so the dbt
    # session-feature mart (abandonment model) can aggregate per session.
    tagged = tag_sessions(events)
    write_table(tagged.select("visitorid", "event", "itemid", "categoryid",
                              "event_date", "session_id"),
                "raw_events", url, props)
    write_table(sessions, "raw_sessions", url, props)
    write_table(categories, "raw_category_tree", url, props)
    write_table(latest_item_snapshot(item_props), "item_latest", url, props)
    write_table(cooccurrence_pairs(events), "cooccur_pairs", url, props)

    return sessions.count()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="data/silver")
    ap.add_argument("--bronze-dir", default="data/bronze")
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
        rows = run(spark, args.silver_dir, args.bronze_dir, url, props)
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
