select
    categoryid,
    parentid,
    parentid is null as is_root
from {{ ref('stg_category_tree') }}
