import argparse
import os

import joblib
import pandas as pd
import psycopg2
import yaml
from sklearn.metrics import precision_score, recall_score, roc_auc_score

HERE = os.path.dirname(__file__)
REGISTRY = os.path.join(HERE, "..", "model_registry")

def build_model(algorithm, params):
    # models live behind one interface (fit/predict/predict_proba) so the training
    # loop below never changes when the algorithm does.
    if algorithm == "xgboost":
        from xgboost import XGBClassifier
        return XGBClassifier(eval_metric="logloss", **params)
    if algorithm == "lightgbm":
        from lightgbm import LGBMClassifier
        return LGBMClassifier(**params)
    if algorithm == "random_forest":
        from sklearn.ensemble import RandomForestClassifier
        return RandomForestClassifier(**params)
    if algorithm == "logistic":
        from sklearn.linear_model import LogisticRegression
        return LogisticRegression(**params)
    raise ValueError(f"unknown algorithm: {algorithm}")

def load_sessions():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "retailrocket"),
        user=os.getenv("POSTGRES_USER", "retail"),
        password=os.getenv("POSTGRES_PASSWORD", "retail"))
    # only sessions that added to cart are "at risk" of abandonment
    df = pd.read_sql(
        "select * from gold.feature_sessions where n_carts > 0 order by start_time", conn)
    conn.close()
    return df

def time_split(df, test_fraction):
    # split by time, not randomly: train on earlier sessions, test on later ones.
    # a random split would let the model peek at behaviour from the same period it's
    # scored on, overstating how it does on genuinely future traffic.
    cut = int(len(df) * (1 - test_fraction))
    return df.iloc[:cut], df.iloc[cut:]

def evaluate(model, X_test, y_test):
    proba = model.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)
    return {
        "precision": round(precision_score(y_test, pred, zero_division=0), 4),
        "recall": round(recall_score(y_test, pred, zero_division=0), 4),
        "auc": round(roc_auc_score(y_test, proba), 4),
    }

def run(cfg, algorithm):
    df = load_sessions()
    features = cfg["features"]
    train_df, test_df = time_split(df, cfg["test_fraction"])

    X_train, y_train = train_df[features], train_df["abandoned"].astype(int)
    X_test, y_test = test_df[features], test_df["abandoned"].astype(int)

    model = build_model(algorithm, cfg["params"].get(algorithm, {}))
    model.fit(X_train, y_train)
    metrics = evaluate(model, X_test, y_test)

    os.makedirs(REGISTRY, exist_ok=True)
    joblib.dump({"model": model, "features": features},
                os.path.join(REGISTRY, f"abandon_{algorithm}.pkl"))
    return metrics, len(train_df), len(test_df)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=os.path.join(HERE, "config.yaml"))
    ap.add_argument("--all", action="store_true", help="train every algorithm and compare")
    args = ap.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    algos = ["xgboost", "random_forest", "logistic"] if args.all else [cfg["algorithm"]]
    for algo in algos:
        metrics, n_tr, n_te = run(cfg, algo)
        print(f"{algo:14s} train={n_tr} test={n_te}  {metrics}")

if __name__ == "__main__":
    main()
