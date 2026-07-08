
  
    

  create  table "retailrocket"."gold"."feature_cooccur__dbt_tmp"
  
  
    as
  
  (
    -- co-view / co-purchase item pairs for the recommender. surrogate pair_id so the
-- (item_a, item_b, pair_type) grain can be tested unique.
select
    item_a || '-' || item_b || '-' || pair_type as pair_id,
    item_a,
    item_b,
    pair_type,
    weight
from "retailrocket"."gold"."stg_cooccur"
  );
  