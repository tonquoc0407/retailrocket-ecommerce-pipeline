
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    pair_id as unique_field,
    count(*) as n_records

from "retailrocket"."gold"."feature_cooccur"
where pair_id is not null
group by pair_id
having count(*) > 1



  
  
      
    ) dbt_internal_test