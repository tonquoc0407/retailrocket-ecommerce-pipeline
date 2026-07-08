-- view -> cart -> purchase counts per category per day, with the two step
-- conversion rates. events with no category (predate the item's first snapshot)
-- can't be attributed, so they're dropped here.
with counts as (
    select
        categoryid,
        event_date,
        count(*) filter (where event = 'view') as views,
        count(*) filter (where event = 'addtocart') as carts,
        count(*) filter (where event = 'transaction') as purchases
    from {{ ref('stg_events') }}
    where categoryid is not null
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
