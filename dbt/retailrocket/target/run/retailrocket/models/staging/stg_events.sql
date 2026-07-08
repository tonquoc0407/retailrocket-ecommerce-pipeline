
  create view "retailrocket"."gold"."stg_events__dbt_tmp"
    
    
  as (
    select
    visitorid,
    event,
    itemid,
    categoryid,
    event_date
from "retailrocket"."public"."raw_events"
  );