import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from api.main import app  # noqa: E402


def test_routes_registered():
    paths = set(app.openapi()["paths"].keys())
    assert "/recommend/{item_id}" in paths
    assert "/funnel-stats" in paths
    assert "/predict-abandon" in paths
    assert "/pipeline-health" in paths
    assert "/metrics" in paths
