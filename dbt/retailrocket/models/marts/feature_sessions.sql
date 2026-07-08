-- per-session features + abandonment label for the classifier.
-- features are built from pre-purchase events only (event != 'transaction') so the
-- outcome doesn't leak into its own predictors. the label comes from fct_sessions.
with browse as (
    select session_id, itemid, categoryid, event
    from {{ ref('stg_events') }}
    where event != 'transaction'
),

agg as (
    select
        session_id,
        count(*) as event_count,
        count(*) filter (where event = 'view') as n_views,
        count(*) filter (where event = 'addtocart') as n_carts,
        count(distinct itemid) as n_items,
        count(distinct categoryid) as n_categories
    from browse
    group by session_id
)

select
    s.session_id,
    s.visitorid,
    s.start_time,
    extract(hour from s.start_time) as start_hour,
    a.event_count,
    a.n_views,
    a.n_carts,
    a.n_items,
    a.n_categories,
    case when a.n_items > 0 then a.n_views::float / a.n_items else 0 end as views_per_item,
    s.has_purchase,
    (a.n_carts > 0 and not s.has_purchase) as abandoned
from {{ ref('fct_sessions') }} s
join agg a on s.session_id = a.session_id
