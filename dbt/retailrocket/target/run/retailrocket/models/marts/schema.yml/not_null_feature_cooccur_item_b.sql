
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select item_b
from "retailrocket"."gold"."feature_cooccur"
where item_b is null



  
  
      
    ) dbt_internal_test