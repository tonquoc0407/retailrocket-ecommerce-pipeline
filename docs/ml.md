# Models

Two models, both fed from the Gold layer. The metrics below were produced on a synthetic
sample dataset (the pipeline run end-to-end locally); rerun on the real RetailRocket data
to get final numbers. The methodology and code paths are the same either way.

## Recommendation (item → item)

Goal: given an item, return related items. Results are precomputed into the Postgres
`item_recommendations` table (`item_id, rec_item_id, score, rank, method`) and the API
just reads the top-N per item. Cold-start items (not in the table) fall back to the
co-purchase pairs in `feature_cooccur`.

**Baseline — ALS (`train_als.py`).** Implicit-feedback matrix factorization over
`visitorid × itemid`, with interactions weighted by intent: `view=1, addtocart=3,
transaction=5` (a purchase is a much stronger preference signal than a view). Item vectors
come from the fitted `itemFactors`; related items are the nearest by cosine similarity.

**Alternative — item2vec (`train_item2vec.py`).** Word2Vec over per-session item
sequences (each session is a "sentence"), giving an embedding per item; neighbours are again
cosine-nearest.

Both write through the same `base.top_n_neighbors` + `save_recommendations` interface and
tag their rows with `method`, so they coexist in one table and a third approach (e.g.
session-based sequence models) could be added without touching the API contract. The
similarity step is O(n²), so the item set is capped (`CANDIDATE_CAP`) before the pairwise
join — a real deployment would swap in an ANN index (FAISS/annoy).

Both methods produced top-20 lists for all 500 sample items. There's no offline ranking
metric wired up yet (would need held-out next-item evaluation) — an extension point.

## Cart-abandonment prediction

Goal: for a session that added something to cart, predict whether it abandons (no purchase).

**Label.** `abandoned = has addtocart and not has_purchase`, from `feature_sessions`. Only
sessions with a cart are "at risk", so training filters to `n_carts > 0`.

**Features.** Built in the `feature_sessions` dbt model from **pre-purchase events only**
(`event != 'transaction'`) so the outcome can't leak into its own predictors:
`start_hour, event_count, n_views, n_carts, n_items, n_categories, views_per_item`.

**Split.** Time-based, not random: earliest 80% of sessions train, most recent 20% test.
A random split would let the model learn from the same period it's scored on and overstate
performance on genuinely future traffic.

**Algorithm is config-driven.** `config.yaml` picks `xgboost | lightgbm | random_forest |
logistic`; `build_model` maps the name to an estimator behind a common fit/predict
interface, so the training loop never changes. XGBoost is the baseline.

### Results (synthetic sample, 2009 train / 503 test at-risk sessions)

| algorithm | precision | recall | AUC |
|---|---|---|---|
| xgboost (baseline) | 0.522 | 0.527 | 0.637 |
| random_forest | 0.546 | 0.493 | 0.643 |
| logistic | 0.554 | 0.420 | 0.636 |

On this synthetic data the three are close, with XGBoost and random forest edging out
logistic on AUC and XGBoost giving the best recall. The point of the config switch is to
make this comparison a one-line change; on the real dataset the ordering may differ. Retrain
all three with `python ml/abandonment/train.py --all`.
