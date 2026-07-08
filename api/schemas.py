from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel

class RecItem(BaseModel):
    rec_item_id: int
    score: float
    rank: int

class RecommendResponse(BaseModel):
    item_id: int
    source: str          # "recommender" or "cooccur_fallback"
    method: Optional[str] = None
    items: list[RecItem]

class FunnelRow(BaseModel):
    category_id: int
    event_date: date
    views: int
    carts: int
    purchases: int
    cart_rate: float
    purchase_rate: float

class TopItem(BaseModel):
    itemid: int
    categoryid: Optional[int] = None
    views: int
    purchases: int
    item_purchase_rate: float

class AbandonFeatures(BaseModel):
    start_hour: int
    event_count: int
    n_views: int
    n_carts: int
    n_items: int
    n_categories: int
    views_per_item: float

class AbandonResponse(BaseModel):
    abandon_probability: float

class PipelineRun(BaseModel):
    task_name: str
    status: str
    rows_processed: Optional[int] = None
    duration_seconds: Optional[float] = None
    started_at: datetime
    error_message: Optional[str] = None
