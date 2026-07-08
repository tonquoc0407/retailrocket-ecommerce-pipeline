
    
    

select
    itemid as unique_field,
    count(*) as n_records

from "retailrocket"."gold"."feature_table"
where itemid is not null
group by itemid
having count(*) > 1


