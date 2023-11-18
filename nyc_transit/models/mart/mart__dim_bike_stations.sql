select 

    DISTINCT
    start_station_id as station_id,
    start_station_name  as station_name, 
    start_lat as  station_lat,
    start_lng as station_lng
from {{ref ('stg__bike_data')}}
union
select
    DISTINCT
    end_station_id as station_id,
    end_station_name as station_name, 
    end_lat as station_lat,
    end_lng as station_lng
from {{ref ('stg__bike_data')}}
