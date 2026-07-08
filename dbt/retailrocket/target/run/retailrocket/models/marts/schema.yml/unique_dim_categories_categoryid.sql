
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    categoryid as unique_field,
    count(*) as n_records

from "retailrocket"."gold"."dim_categories"
where categoryid is not null
group by categoryid
having count(*) > 1



  
  
      
    ) dbt_internal_test