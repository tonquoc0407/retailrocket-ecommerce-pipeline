
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select item_a
from "retailrocket"."gold"."feature_cooccur"
where item_a is null



  
  
      
    ) dbt_internal_test