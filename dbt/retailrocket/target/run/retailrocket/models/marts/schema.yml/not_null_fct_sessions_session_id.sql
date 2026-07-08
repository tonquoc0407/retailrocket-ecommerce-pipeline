
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_id
from "retailrocket"."gold"."fct_sessions"
where session_id is null



  
  
      
    ) dbt_internal_test