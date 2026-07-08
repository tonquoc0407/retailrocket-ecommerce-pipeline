select
    categoryid,
    parentid
from {{ source('raw', 'raw_category_tree') }}
