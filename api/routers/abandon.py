import logging
import os

import joblib
import yaml
from fastapi import APIRouter, HTTPException

from api.schemas import AbandonFeatures, AbandonResponse

log = logging.getLogger("api")
router = APIRouter()

_ML = os.path.join(os.path.dirname(__file__), "..", "..", "ml", "abandonment")
_bundle = None

def load_model():
    # called at startup; the model may not be trained yet on a fresh stack, so
    # don't crash the API — just serve 503 from the endpoint until it exists.
    global _bundle
    with open(os.path.join(_ML, "config.yaml")) as f:
        algo = yaml.safe_load(f)["algorithm"]
    path = os.path.join(_ML, "..", "model_registry", f"abandon_{algo}.pkl")
    try:
        _bundle = joblib.load(path)
    except FileNotFoundError:
        log.warning("abandonment model not found at %s; /predict-abandon will 503", path)

@router.post("/predict-abandon", response_model=AbandonResponse)
def predict_abandon(feats: AbandonFeatures):
    if _bundle is None:
        raise HTTPException(status_code=503, detail="abandonment model not trained yet")
    row = [[getattr(feats, f) for f in _bundle["features"]]]
    proba = _bundle["model"].predict_proba(row)[0][1]
    return AbandonResponse(abandon_probability=round(float(proba), 4))
