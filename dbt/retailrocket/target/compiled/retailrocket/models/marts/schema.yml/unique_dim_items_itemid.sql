
    
    

select
    itemid as unique_field,
    count(*) as n_records

from "retailrocket"."gold"."dim_items"
where itemid is not null
group by itemid
having count(*) > 1


