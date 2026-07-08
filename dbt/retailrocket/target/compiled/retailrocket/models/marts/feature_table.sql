-- item-level features feeding both models: popularity + conversion for the
-- recommender's cold-start fallback and the abandonment model. co-occurrence
-- pairs are a different grain, so they live in feature_cooccur, not here.
with item_counts as (
    select
        itemid,
        count(*) filter (where event = 'view') as views,
        count(*) filter (where event = 'addtocart') as carts,
        count(*) filter (where event = 'transaction') as purchases
    from "retailrocket"."gold"."stg_events"
    group by itemid
),

cat_conv as (
    select
        categoryid,
        count(*) filter (where event = 'view') as cat_views,
        count(*) filter (where event = 'transaction') as cat_purchases
    from "retailrocket"."gold"."stg_events"
    where categoryid is not null
    group by categoryid
)

select
    ic.itemid,
    il.categoryid,
    ic.views,
    ic.carts,
    ic.purchases,
    ic.views as popularity,
    case when ic.views > 0 then ic.purchases::float / ic.views else 0 end as item_purchase_rate,
    case when cc.cat_views > 0 then cc.cat_purchases::float / cc.cat_views else 0 end as category_purchase_rate
from item_counts ic
left join "retailrocket"."gold"."stg_item_latest" il on ic.itemid = il.itemid
left join cat_conv cc on il.categoryid = cc.categoryid