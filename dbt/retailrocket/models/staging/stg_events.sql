select
    visitorid,
    event,
    itemid,
    categoryid,
    event_date,
    session_id
from {{ source('raw', 'raw_events') }}
