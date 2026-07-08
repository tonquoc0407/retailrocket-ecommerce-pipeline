import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))
import bronze_ingest  # noqa: E402
import silver_transform  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test_silver").getOrCreate()
    yield s
    s.stop()

def read_events(spark, name):
    df = bronze_ingest.read_csv(spark, f"{FIXTURES}/{name}", bronze_ingest.EVENTS_SCHEMA)
    return df.withColumn("event_date", F.to_date(F.from_unixtime(F.col("timestamp") / 1000)))

def read_props(spark, name):
    return bronze_ingest.read_csv(spark, f"{FIXTURES}/{name}", bronze_ingest.ITEM_PROPS_SCHEMA)

def test_dedupe_drops_exact_duplicates(spark):
    events = read_events(spark, "silver_dupes.csv")
    assert silver_transform.dedupe_events(events).count() == 2

def test_point_in_time_no_future_leak(spark):
    # item 100 changes category: 10 at ts=1000, then 20 at ts=5000
    props = read_props(spark, "silver_props.csv")
    events = read_events(spark, "silver_events.csv")

    got = {r["timestamp"]: r["categoryid"]
           for r in silver_transform.point_in_time_category(events, props).collect()}

    assert got[500] is None     # before any snapshot
    assert got[1000] == 10      # exactly at first snapshot (at-or-before includes equal)
    assert got[2000] == 10      # the key assertion: no leakage of the ts=5000 value
    assert got[6000] == 20      # after second snapshot
