with source as (

    select * from {{ source('main', 'central_park_weather') }}

),

renamed as (

    select
        station,
        name,
        date::date as date,
        awnd::double as awnd,
        prcp::double as prcp,
       /*replace nulls by 0,*/
        (CASE WHEN snow IS NULL THEN 0 ELSE snow END)::double AS snow,   
        snwd::double as snwd,
        tmax::int as tmax,
        tmin::int as tmin,
        filename

    from source

)

select 
    date,
    awnd,
    prcp,
    snow,
    snwd,
    tmax,
    tmin,
    filename
from renamed
where snow is not null  


