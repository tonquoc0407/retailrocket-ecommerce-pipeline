import os

import joblib
import yaml
from fastapi import APIRouter

from api.schemas import AbandonFeatures, AbandonResponse

router = APIRouter()

_ML = os.path.join(os.path.dirname(__file__), "..", "..", "ml", "abandonment")
_bundle = None


def load_model():
    global _bundle
    with open(os.path.join(_ML, "config.yaml")) as f:
        algo = yaml.safe_load(f)["algorithm"]
    _bundle = joblib.load(os.path.join(_ML, "..", "model_registry", f"abandon_{algo}.pkl"))


@router.post("/predict-abandon", response_model=AbandonResponse)
def predict_abandon(feats: AbandonFeatures):
    row = [[getattr(feats, f) for f in _bundle["features"]]]
    proba = _bundle["model"].predict_proba(row)[0][1]
    return AbandonResponse(abandon_probability=round(float(proba), 4))
