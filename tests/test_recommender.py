import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ml", "recommender"))
import bronze_ingest  # noqa: E402
import base  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test_recommender").getOrCreate()
    yield s
    s.stop()

def test_implicit_ratings_weight_events(spark):
    events = bronze_ingest.read_csv(spark, f"{FIXTURES}/cooccur_events.csv",
                                    bronze_ingest.EVENTS_SCHEMA)
    ratings = {(r["visitorid"], r["itemid"]): r["rating"]
               for r in base.build_implicit_ratings(events).collect()}
    assert ratings[(1, 100)] == 6   # view(1) + transaction(5)
    assert ratings[(1, 200)] == 6
    assert ratings[(1, 300)] == 1   # view only

def test_top_n_neighbors_by_cosine(spark):
    df = spark.read.csv(f"{FIXTURES}/rec_vectors.csv", header=True, inferSchema=True)
    vectors = df.select(F.col("id").cast("long"),
                        F.array(F.col("x").cast("double"), F.col("y").cast("double")).alias("vec"))
    recs = base.top_n_neighbors(vectors, top_n=1).collect()
    top = {r["item_id"]: r["rec_item_id"] for r in recs}
    assert top[1] == 2   # (1,0) closest to (0.9,0.1), not the orthogonal (0,1)
    assert top[3] == 2   # (0,1) closest to (0.9,0.1) than to (1,0)
