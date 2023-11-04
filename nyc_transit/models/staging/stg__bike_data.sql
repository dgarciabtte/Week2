/*
   dgarciab - Diana Maria Garcia Bustamante
   references pages:  https://mode.com/sql-tutorial/sql-string-functions-for-cleaning/#coalesce
*/

with source as (

	select * from {{source ('main', 'bike_data') }}

),
renamed as (

        select 
        TRIM(tripduration)::bigint as Trip_Duration,
        TRIM(starttime)::TIMESTAMP  as Start_Time,  
        TRIM(stoptime)::TIMESTAMP  as Stop_Time,
        COALESCE(TRIM("start_station_id"), TRIM("start station id")) AS Start_Station_Id,
        COALESCE(TRIM("start station name"), TRIM("start_station_name")) AS Start_Station_Name,
        COALESCE(TRIM ("start station latitude"),TRIM ("start_lat"))::double as Start_station_Latitude, 
        COALESCE(TRIM ("start station longitude"), TRIM("start_lng"))::double as Start_station_Longitude, 
   
        COALESCE(TRIM("end_station_id"), TRIM("end station id")) AS End_Station_Id,
        COALESCE(TRIM("end station name"), TRIM("end_station_name")) AS End_Station_Name,
        COALESCE(TRIM ("end station latitude"), TRIM ("end_lat"))::double as End_station_Latitude, 
        COALESCE(TRIM ("end station longitude"), TRIM ("end_lng"))::double as End_station_Longitude, 

        TRIM(bikeid)::  bigint as Bike_Id,
        TRIM(usertype) as User_Type,
        TRIM("birth year"):: int as Birth_Year,
        TRIM(gender):: int as Gender,
        TRIM(ride_id) as Ride_id,
        TRIM(rideable_type) as Bike_Type,
        TRIM(started_at):: TIMESTAMP  as Started_At,
        TRIM(ended_at):: TIMESTAMP  as End_At,
        TRIM(member_casual) as Type_of_Rider,
        filename 
        
        from source	
)

select * from renamed



