select
    session_id,
    visitorid,
    start_time,
    end_time,
    event_count,
    has_purchase
from {{ source('raw', 'raw_sessions') }}
