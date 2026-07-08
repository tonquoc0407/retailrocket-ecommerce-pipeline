
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select itemid
from "retailrocket"."gold"."dim_items"
where itemid is null



  
  
      
    ) dbt_internal_test