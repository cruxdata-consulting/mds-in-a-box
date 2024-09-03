with countries as (
    select * from {{ ref('country_code_mapping') }}
)

select * from countries