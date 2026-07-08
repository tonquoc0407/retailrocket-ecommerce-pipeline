
  create view "retailrocket"."gold"."stg_cooccur__dbt_tmp"
    
    
  as (
    select
    item_a,
    item_b,
    pair_type,
    weight
from "retailrocket"."public"."cooccur_pairs"
  );