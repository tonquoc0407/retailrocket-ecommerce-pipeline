
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select categoryid as from_field
    from "retailrocket"."gold"."fct_funnel"
    where categoryid is not null
),

parent as (
    select categoryid as to_field
    from "retailrocket"."gold"."dim_categories"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test