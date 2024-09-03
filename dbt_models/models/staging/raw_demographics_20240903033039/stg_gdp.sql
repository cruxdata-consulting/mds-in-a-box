with source as (
    select * from {{ source('raw_demographics_20240903033039', 'gdp') }}
),

renamed as (
    select 
        country_name as country_name
        , country_code as country_code
        , indicator_name as indicator_name
        , indicator_code as indicator_code
        , year as year
        , value as value
        , _dlt_load_id as _dlt_load_id
        , _dlt_id as _dlt_id
    from source
)

select * from renamed