-- item dimension: latest category per item, joined to its category's parent
select
    i.itemid,
    i.categoryid,
    c.parentid
from "retailrocket"."gold"."stg_item_latest" i
left join "retailrocket"."gold"."stg_category_tree" c on i.categoryid = c.categoryid