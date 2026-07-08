
  
    

  create  table "retailrocket"."gold"."fct_sessions__dbt_tmp"
  
  
    as
  
  (
    select
    session_id,
    visitorid,
    start_time,
    end_time,
    extract(epoch from (end_time - start_time)) as duration_seconds,
    event_count,
    has_purchase
from "retailrocket"."gold"."stg_sessions"
  );
  