from datetime import date
from typing import Optional

from fastapi import APIRouter, Query

from api.db import get_cursor, GOLD_SCHEMA
from api.schemas import FunnelRow

router = APIRouter()


@router.get("/funnel-stats", response_model=list[FunnelRow])
def funnel_stats(
    category_id: Optional[int] = None,
    from_: Optional[date] = Query(None, alias="from"),
    to: Optional[date] = None,
):
    where = []
    params = {}
    if category_id is not None:
        where.append("categoryid = %(cat)s")
        params["cat"] = category_id
    if from_ is not None:
        where.append("event_date >= %(from)s")
        params["from"] = from_
    if to is not None:
        where.append("event_date <= %(to)s")
        params["to"] = to
    clause = ("where " + " and ".join(where)) if where else ""

    with get_cursor() as cur:
        cur.execute(
            f"""
            select categoryid as category_id, event_date, views, carts, purchases,
                   cart_rate, purchase_rate
            from {GOLD_SCHEMA}.fct_funnel
            {clause}
            order by event_date, categoryid
            """,
            params,
        )
        return [FunnelRow(**r) for r in cur.fetchall()]
