
  create view "retailrocket"."gold"."stg_item_latest__dbt_tmp"
    
    
  as (
    select
    itemid,
    categoryid
from "retailrocket"."public"."item_latest"
  );