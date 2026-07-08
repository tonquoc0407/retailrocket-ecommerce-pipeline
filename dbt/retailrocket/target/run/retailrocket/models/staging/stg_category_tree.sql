
  create view "retailrocket"."gold"."stg_category_tree__dbt_tmp"
    
    
  as (
    select
    categoryid,
    parentid
from "retailrocket"."public"."raw_category_tree"
  );