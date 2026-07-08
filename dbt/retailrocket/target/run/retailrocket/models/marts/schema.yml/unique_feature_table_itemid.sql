
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    itemid as unique_field,
    count(*) as n_records

from "retailrocket"."gold"."feature_table"
where itemid is not null
group by itemid
having count(*) > 1



  
  
      
    ) dbt_internal_test