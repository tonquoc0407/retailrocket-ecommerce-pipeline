
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pair_id
from "retailrocket"."gold"."feature_cooccur"
where pair_id is null



  
  
      
    ) dbt_internal_test