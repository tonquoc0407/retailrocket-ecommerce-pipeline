from fastapi import APIRouter, Query

from api.db import get_cursor, GOLD_SCHEMA
from api.schemas import RecItem, RecommendResponse

router = APIRouter()

def _fallback(item_id, n):
    # cold-start: item has no trained recommendations, so use co-purchase neighbours
    # from the gold pairs table (either side of the pair).
    with get_cursor() as cur:
        cur.execute(
            f"""
            select case when item_a = %(id)s then item_b else item_a end as rec_item_id,
                   weight as score,
                   row_number() over (order by weight desc) as rank
            from {GOLD_SCHEMA}.feature_cooccur
            where pair_type = 'purchase' and (item_a = %(id)s or item_b = %(id)s)
            order by weight desc
            limit %(n)s
            """,
            {"id": item_id, "n": n},
        )
        return cur.fetchall()

@router.get("/recommend/{item_id}", response_model=RecommendResponse)
def recommend(item_id: int, method: str = "als", n: int = Query(10, ge=1, le=100)):
    with get_cursor() as cur:
        cur.execute(
            """
            select rec_item_id, score, rank
            from item_recommendations
            where item_id = %s and method = %s
            order by rank
            limit %s
            """,
            (item_id, method, n),
        )
        rows = cur.fetchall()

    if rows:
        return RecommendResponse(item_id=item_id, source="recommender", method=method,
                                 items=[RecItem(**r) for r in rows])

    rows = _fallback(item_id, n)
    return RecommendResponse(item_id=item_id, source="cooccur_fallback",
                             items=[RecItem(**r) for r in rows])
