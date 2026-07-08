
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select categoryid
from "retailrocket"."gold"."dim_categories"
where categoryid is null



  
  
      
    ) dbt_internal_test