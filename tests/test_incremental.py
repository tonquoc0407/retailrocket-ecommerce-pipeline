import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))
import bronze_ingest  # noqa: E402
import session_builder  # noqa: E402
import silver_transform  # noqa: E402

# visitor 1 views at 23:50 on day 1 and adds to cart at 00:05 on day 2 -- 15 minutes
# apart, so it is one session that straddles midnight. visitors 2 and 3 have a single
# event each, one on either day.
FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "midnight")

DAY1, DAY2, DAY3 = "2015-06-01", "2015-06-02", "2015-06-03"
STRADDLING_SESSION = "1-1433202600000"  # visitorid - first timestamp of the session

@pytest.fixture(scope="module")
def spark():
    # the fixture timestamps are UTC and the assertions below are about which side of
    # midnight an event falls on, so pin the session timezone rather than inheriting the
    # machine's.
    s = (SparkSession.builder
         .master("local[1]")
         .appName("test_incremental")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    yield s
    s.stop()

def daily_run(spark, root, since, until):
    # one airflow dagrun: every stage gets the same [since, until)
    bronze, silver = f"{root}/bronze", f"{root}/silver"
    bronze_ingest.run(spark, FIXTURES, bronze, since=since, until=until)
    silver_transform.run(spark, bronze, silver, since=since, until=until)
    session_builder.run(spark, silver, silver, since=since, until=until)

def full_run(spark, root):
    bronze, silver = f"{root}/bronze", f"{root}/silver"
    bronze_ingest.run(spark, FIXTURES, bronze)
    silver_transform.run(spark, bronze, silver)
    session_builder.run(spark, silver, silver)

def sessions_by_id(spark, root):
    rows = spark.read.parquet(f"{root}/silver/sessions").collect()
    return {r["session_id"]: r for r in rows}

def test_first_incremental_run_bootstraps_the_dimensions(spark, tmp_path):
    # a backfill's first dagrun has no full load behind it, so bronze has to land the
    # static tables itself or silver reads a path that doesn't exist
    root = str(tmp_path / "d1")
    daily_run(spark, root, DAY1, DAY2)

    assert spark.read.parquet(f"{root}/bronze/item_properties").count() == 3
    assert spark.read.parquet(f"{root}/bronze/category_tree").count() == 3

def test_midnight_session_is_completed_by_the_next_run(spark, tmp_path):
    root = str(tmp_path / "days")

    # day 1 sees only the 23:50 view -- the rest of the session hasn't been ingested yet,
    # so the session is written short. that is expected and is what day 2 repairs.
    daily_run(spark, root, DAY1, DAY2)
    day1 = sessions_by_id(spark, root)
    assert set(day1) == {STRADDLING_SESSION, "2-1433152800000"}
    assert day1[STRADDLING_SESSION]["event_count"] == 1

    # day 2 reads back over day 1, rebuilds its session_date partition and overwrites it
    daily_run(spark, root, DAY2, DAY3)
    day2 = sessions_by_id(spark, root)

    healed = day2[STRADDLING_SESSION]
    assert healed["event_count"] == 2  # the view and the add-to-cart, now one session
    assert str(healed["session_date"]) == DAY1  # owned by the day it started
    assert healed["has_purchase"] is False

    # and the repair replaced day 1's partition rather than appending to it
    assert len(day2) == 3

def test_incremental_matches_a_full_rebuild(spark, tmp_path):
    # the point of the whole exercise: running day by day has to land exactly what one
    # pass over the same data would.
    inc, full = str(tmp_path / "inc"), str(tmp_path / "full")

    daily_run(spark, inc, DAY1, DAY2)
    daily_run(spark, inc, DAY2, DAY3)
    full_run(spark, full)

    got, want = sessions_by_id(spark, inc), sessions_by_id(spark, full)
    assert got.keys() == want.keys()
    for session_id, want_row in want.items():
        assert got[session_id].asDict() == want_row.asDict()

def test_session_id_does_not_depend_on_how_much_history_is_read(spark, tmp_path):
    # session_id used to be visitorid + a running count, which restarts at 1 inside
    # whatever window the job happened to read: day 2's first session would collide with
    # day 1's. keying it on the session's first timestamp is what makes the partition
    # overwrites above an update instead of a duplicate.
    root = str(tmp_path / "ids")
    daily_run(spark, root, DAY1, DAY2)
    daily_run(spark, root, DAY2, DAY3)

    ids = list(sessions_by_id(spark, root))
    assert len(ids) == len(set(ids)) == 3
    assert STRADDLING_SESSION in ids
