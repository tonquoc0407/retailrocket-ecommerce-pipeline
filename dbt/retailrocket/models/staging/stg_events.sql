select
    visitorid,
    event,
    itemid,
    categoryid,
    event_date
from {{ source('raw', 'raw_events') }}
