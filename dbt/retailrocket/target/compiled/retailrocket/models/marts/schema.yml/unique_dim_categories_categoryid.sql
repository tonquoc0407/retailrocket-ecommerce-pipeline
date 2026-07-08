
    
    

select
    categoryid as unique_field,
    count(*) as n_records

from "retailrocket"."gold"."dim_categories"
where categoryid is not null
group by categoryid
having count(*) > 1


