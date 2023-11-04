
/*
   dgarciab - Diana Maria Garcia Bustamante
   references pages:  https://mode.com/sql-tutorial/sql-string-functions-for-cleaning/
*/


with source as (

	select * from {{source ('main', 'central_park_weather') }}

),
renamed as (

        select 
	    TRIM(station) as Network_ID,
	    TRIM(name) as Station_Name,
            TRIM(date)::date as Date_Sample,
            TRIM(AWND)::double as Avg_daily_wind_spd,
            TRIM(prcp)::double as Precipitation,
            TRIM(SNOW)::double as Snowfall,
            TRIM(SNWD)::double as Snow_depth, 
            TRIM(TMAX):: int as Max_Temp,
            TRIM(TMIN):: int as Min_Temp,
            filename

        from source	
)

select * from renamed



