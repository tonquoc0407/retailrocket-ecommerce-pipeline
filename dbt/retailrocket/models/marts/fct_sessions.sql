select
    session_id,
    visitorid,
    start_time,
    end_time,
    extract(epoch from (end_time - start_time)) as duration_seconds,
    event_count,
    has_purchase
from {{ ref('stg_sessions') }}
