select
    itemid,
    categoryid
from {{ source('raw', 'item_latest') }}
