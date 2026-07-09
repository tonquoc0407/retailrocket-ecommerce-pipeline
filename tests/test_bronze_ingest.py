import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))
import bronze_ingest  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test_bronze").getOrCreate()
    yield s
    s.stop()

def test_bronze_writes_all_tables(spark, tmp_path):
    out = str(tmp_path / "bronze")
    rows = bronze_ingest.run(spark, FIXTURES, out)

    assert rows == 8  # matches fixture events.csv

    events = spark.read.parquet(f"{out}/events")
    assert "event_date" in events.columns
    # two of the events fall on a later day -> more than one partition
    assert events.select("event_date").distinct().count() == 2

    props = spark.read.parquet(f"{out}/item_properties")
    assert props.count() == 7  # part1 (4) + part2 (3)

    cats = spark.read.parquet(f"{out}/category_tree")
    assert cats.count() == 3

def test_bronze_incremental_only_rewrites_new_partitions(spark, tmp_path):
    out = str(tmp_path / "bronze")
    bronze_ingest.run(spark, FIXTURES, out)  # full load first

    dates = sorted(
        r[0] for r in
        spark.read.parquet(f"{out}/events").select("event_date").distinct().collect()
    )
    later = dates[-1]

    # re-ingest only the last day
    written = bronze_ingest.run(spark, FIXTURES, out, since=str(later))
    assert written == 2  # the two events on the later day, not all 8

    # the earlier partition is left in place, so both days still exist
    after = spark.read.parquet(f"{out}/events").select("event_date").distinct().count()
    assert after == 2
