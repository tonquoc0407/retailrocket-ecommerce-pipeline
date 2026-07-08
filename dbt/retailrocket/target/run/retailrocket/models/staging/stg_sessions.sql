
  create view "retailrocket"."gold"."stg_sessions__dbt_tmp"
    
    
  as (
    select
    session_id,
    visitorid,
    start_time,
    end_time,
    event_count,
    has_purchase
from "retailrocket"."public"."raw_sessions"
  );