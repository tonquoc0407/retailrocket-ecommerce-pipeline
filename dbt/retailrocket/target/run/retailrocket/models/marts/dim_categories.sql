
  
    

  create  table "retailrocket"."gold"."dim_categories__dbt_tmp"
  
  
    as
  
  (
    select
    categoryid,
    parentid,
    parentid is null as is_root
from "retailrocket"."gold"."stg_category_tree"
  );
  