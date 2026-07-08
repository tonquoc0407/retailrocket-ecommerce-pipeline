import argparse
import sys
import time
from os.path import dirname, join

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS

sys.path.insert(0, join(dirname(__file__), "..", "..", "spark_jobs"))
from pipeline_log import log_run, now_utc  # noqa: E402

import base  # noqa: E402

PG_JDBC = "org.postgresql:postgresql:42.7.3"


def top_items(ratings, cap):
    counts = ratings.groupBy("itemid").agg(F.sum("rating").alias("pop"))
    return counts.orderBy(F.col("pop").desc()).limit(cap).select("itemid")


def train(spark, silver_dir):
    events = spark.read.parquet(f"{silver_dir}/events_enriched")
    ratings = base.build_implicit_ratings(events)

    als = ALS(userCol="visitorid", itemCol="itemid", ratingCol="rating",
              implicitPrefs=True, rank=32, maxIter=10, regParam=0.1,
              coldStartStrategy="drop", nonnegative=True)
    model = als.fit(ratings)

    keep = top_items(ratings, base.CANDIDATE_CAP)
    # ALS itemFactors.features is already an array<float>, just widen it to double
    vectors = (model.itemFactors
               .select(F.col("id"), F.col("features").cast("array<double>").alias("vec"))
               .join(keep, F.col("id") == keep["itemid"])
               .select("id", "vec"))

    return base.top_n_neighbors(vectors)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="data/silver")
    ap.add_argument("--no-log", action="store_true")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("train_als")
             .config("spark.jars.packages", PG_JDBC).getOrCreate())
    url, props = base.pg_config()

    started = now_utc()
    t0 = time.perf_counter()
    try:
        recs = train(spark, args.silver_dir)
        n = recs.count()
        base.save_recommendations(recs, "als", url, props)
    except Exception as e:
        if not args.no_log:
            log_run("train_als", None, time.perf_counter() - t0, "failed", started, str(e))
        raise
    finally:
        spark.stop()

    duration = time.perf_counter() - t0
    print(f"train_als wrote {n} recommendations in {duration:.1f}s")
    if not args.no_log:
        log_run("train_als", n, duration, "success", started)


if __name__ == "__main__":
    main()
