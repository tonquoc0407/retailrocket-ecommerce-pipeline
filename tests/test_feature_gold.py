import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))
import bronze_ingest  # noqa: E402
import feature_gold  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test_feature_gold").getOrCreate()
    yield s
    s.stop()

def test_latest_item_snapshot_takes_newest(spark):
    props = bronze_ingest.read_csv(spark, f"{FIXTURES}/silver_props.csv",
                                   bronze_ingest.ITEM_PROPS_SCHEMA)
    rows = feature_gold.latest_item_snapshot(props).collect()
    assert len(rows) == 1
    assert rows[0]["itemid"] == 100
    assert rows[0]["categoryid"] == 20   # newest snapshot (ts=5000), not the ts=1000 one

def test_cooccurrence_pairs(spark):
    events = bronze_ingest.read_csv(spark, f"{FIXTURES}/cooccur_events.csv",
                                    bronze_ingest.EVENTS_SCHEMA)
    pairs = feature_gold.cooccurrence_pairs(events, min_support=1).collect()

    got = {(r["item_a"], r["item_b"], r["pair_type"]): r["weight"] for r in pairs}
    # three items viewed in one session -> three unordered co-view pairs
    assert got[(100, 200, "view")] == 1
    assert got[(100, 300, "view")] == 1
    assert got[(200, 300, "view")] == 1
    # two items bought in the session -> one co-purchase pair
    assert got[(100, 200, "purchase")] == 1
    assert len(got) == 4
