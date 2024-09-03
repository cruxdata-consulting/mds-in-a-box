with int_countries as (
    select * from {{ ref('int_countries') }}
),
stg_gdp as (
    select * from {{ ref('stg_gdp') }}
),
stg_population as (
    select * from {{ ref('stg_population') }}
),
gdp as (
    select
        country_code
        , year as latest_gdp_year
        , cast(value as double) as latest_gdp
        , rank() over (partition by country_code order by year desc) as rnk
        , avg(cast(value as double)) over (
            partition by country_code 
            order by year asc
            rows between 4 preceding and current row
            ) as last_5y_avg_gdp
    from stg_gdp
    where not value=''
),
popn as (
    select 
        iso3_code as iso3166_code
        , time as latest_population_year
        , cast(t_population1_july as double) * 1000 as latest_population
        , rank() over (partition by iso3_code order by time desc) as rnk
        , avg(cast(t_population1_july as double)) over (
            partition by iso3_code 
            order by time asc
            rows between 4 preceding and current row
            ) * 1000 as last_5y_avg_population
    from stg_population
),
countries_with_gdp_population as (
    select
        c.iso3166_code
        , c.olympic__code
        , c.country_name
        , g.latest_gdp_year
        , g.latest_gdp
        , g.last_5y_avg_gdp
        , p.latest_population
        , p.latest_population_year
        , p.last_5y_avg_population
        , g.last_5y_avg_gdp / p.last_5y_avg_population as gdp_per_capita
    from int_countries c
    left join gdp g on c.iso3166_code = g.country_code and g.rnk = 1
    left join popn p on c.iso3166_code = p.iso3166_code and p.rnk = 1
)

select * from countries_with_gdp_population