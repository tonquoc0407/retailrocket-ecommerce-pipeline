import argparse
import sys
import time
from os.path import dirname, join

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import Word2Vec
from pyspark.ml.functions import vector_to_array

sys.path.insert(0, join(dirname(__file__), "..", "..", "spark_jobs"))
from pipeline_log import log_run, now_utc  # noqa: E402
from session_builder import tag_sessions  # noqa: E402

import base  # noqa: E402

PG_JDBC = "org.postgresql:postgresql:42.7.3"


def item_sequences(events):
    # treat each session's item order as a "sentence" for word2vec (item2vec)
    tagged = tag_sessions(events)
    ordered = tagged.groupBy("session_id").agg(
        F.sort_array(F.collect_list(F.struct("timestamp", "itemid"))).alias("s"))
    seqs = ordered.withColumn("items", F.expr("transform(s, x -> cast(x.itemid as string))"))
    return seqs.filter(F.size("items") >= 2).select("items")


def train(spark, silver_dir):
    events = spark.read.parquet(f"{silver_dir}/events_enriched")
    seqs = item_sequences(events)

    w2v = Word2Vec(inputCol="items", outputCol="vec", vectorSize=32,
                   minCount=5, windowSize=5, maxIter=5)
    model = w2v.fit(seqs)

    vectors = model.getVectors().select(
        F.col("word").cast("long").alias("id"), vector_to_array("vector").alias("vec"))

    return base.top_n_neighbors(vectors)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default="data/silver")
    ap.add_argument("--no-log", action="store_true")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("train_item2vec")
             .config("spark.jars.packages", PG_JDBC).getOrCreate())
    url, props = base.pg_config()

    started = now_utc()
    t0 = time.perf_counter()
    try:
        recs = train(spark, args.silver_dir)
        n = recs.count()
        base.save_recommendations(recs, "item2vec", url, props)
    except Exception as e:
        if not args.no_log:
            log_run("train_item2vec", None, time.perf_counter() - t0, "failed", started, str(e))
        raise
    finally:
        spark.stop()

    duration = time.perf_counter() - t0
    print(f"train_item2vec wrote {n} recommendations in {duration:.1f}s")
    if not args.no_log:
        log_run("train_item2vec", n, duration, "success", started)


if __name__ == "__main__":
    main()
