with source as (
    select * from {{ source('raw_demographics_20240903033039', 'population') }}
),

renamed as (
    select 
        sort_order as sort_order
        , loc_id as loc_id
        , notes as notes
        , iso3_code as iso3_code
        , iso2_code as iso2_code
        , sdmx_code as sdmx_code
        , loc_type_id as loc_type_id
        , loc_type_name as loc_type_name
        , parent_id as parent_id
        , location as location
        , var_id as var_id
        , variant as variant
        , time as time
        , t_population1_jan as t_population1_jan
        , t_population1_july as t_population1_july
        , t_population_male1_july as t_population_male1_july
        , t_population_female1_july as t_population_female1_july
        , pop_density as pop_density
        , pop_sex_ratio as pop_sex_ratio
        , median_age_pop as median_age_pop
        , nat_change as nat_change
        , nat_change_rt as nat_change_rt
        , pop_change as pop_change
        , pop_growth_rate as pop_growth_rate
        , doubling_time as doubling_time
        , births as births
        , births1519 as births1519
        , cbr as cbr
        , tfr as tfr
        , nrr as nrr
        , mac as mac
        , srb as srb
        , deaths as deaths
        , deaths_male as deaths_male
        , deaths_female as deaths_female
        , cdr as cdr
        , l_ex as l_ex
        , l_ex_male as l_ex_male
        , l_ex_female as l_ex_female
        , le15 as le15
        , le15_male as le15_male
        , le15_female as le15_female
        , le65 as le65
        , le65_male as le65_male
        , le65_female as le65_female
        , le80 as le80
        , le80_male as le80_male
        , le80_female as le80_female
        , infant_deaths as infant_deaths
        , imr as imr
        , l_bsurviving_age1 as l_bsurviving_age1
        , under5_deaths as under5_deaths
        , q5 as q5
        , q0040 as q0040
        , q0040_male as q0040_male
        , q0040_female as q0040_female
        , q0060 as q0060
        , q0060_male as q0060_male
        , q0060_female as q0060_female
        , q1550 as q1550
        , q1550_male as q1550_male
        , q1550_female as q1550_female
        , q1560 as q1560
        , q1560_male as q1560_male
        , q1560_female as q1560_female
        , net_migrations as net_migrations
        , cnmr as cnmr
        , _dlt_load_id as _dlt_load_id
        , _dlt_id as _dlt_id
    from source
)

select * from renamed