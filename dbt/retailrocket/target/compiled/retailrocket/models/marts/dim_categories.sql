select
    categoryid,
    parentid,
    parentid is null as is_root
from "retailrocket"."gold"."stg_category_tree"