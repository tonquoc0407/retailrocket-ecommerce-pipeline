{{ config(
    materialized='incremental',
    unique_key=['categoryid', 'event_date'],
    incremental_strategy='delete+insert'
) }}

-- view -> cart -> purchase counts per category per day, with the two step
-- conversion rates. events with no category (predate the item's first snapshot)
-- can't be attributed, so they're dropped here.
--
-- incremental: each run recomputes only the tail of the calendar. late events can
-- land on an earlier event_date, so we reprocess a 3-day lookback and delete+insert
-- those (categoryid, event_date) rows instead of appending -- appending would double
-- count a day that's touched twice.
with counts as (
    select
        categoryid,
        event_date,
        count(*) filter (where event = 'view') as views,
        count(*) filter (where event = 'addtocart') as carts,
        count(*) filter (where event = 'transaction') as purchases
    from {{ ref('stg_events') }}
    where categoryid is not null
    {% if is_incremental() %}
      and event_date >= (select max(event_date) from {{ this }}) - interval '3 days'
    {% endif %}
    group by categoryid, event_date
)

select
    categoryid,
    event_date,
    views,
    carts,
    purchases,
    case when views > 0 then carts::float / views else 0 end as cart_rate,
    case when carts > 0 then purchases::float / carts else 0 end as purchase_rate
from counts
