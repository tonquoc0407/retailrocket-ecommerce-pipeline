
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select visitorid
from "retailrocket"."gold"."fct_sessions"
where visitorid is null



  
  
      
    ) dbt_internal_test