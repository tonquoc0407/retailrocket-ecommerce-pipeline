import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))
import bronze_ingest  # noqa: E402
import session_builder  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test_session").getOrCreate()
    yield s
    s.stop()

def test_gap_splits_sessions(spark):
    # visitor 1: two events 10 min apart (one session), then a 33-min gap opens a
    # second session that ends in a purchase. visitor 2: a single event.
    events = bronze_ingest.read_csv(spark, f"{FIXTURES}/session_events.csv",
                                    bronze_ingest.EVENTS_SCHEMA)
    sessions = session_builder.build_sessions(events).collect()

    assert len(sessions) == 3  # 2 for visitor 1, 1 for visitor 2

    v1 = sorted([s for s in sessions if s["visitorid"] == 1], key=lambda s: s["start_time"])
    assert v1[0]["event_count"] == 2
    assert v1[0]["has_purchase"] is False
    assert v1[1]["event_count"] == 2
    assert v1[1]["has_purchase"] is True
