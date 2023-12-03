--- Author: dgarciab

-- this query finds taxi trips which don’t have a pick up location_id in the locations table.

--- functions (CTE) with the trips tables and locations. 
 WITH trips AS (
    SELECT
        type,
        duration_min,
        pulocationid AS init_location,
        dolocationid AS final_location,
        pickup_datetime,
        dropOff_datetime,
        duration_min
    FROM
        {{ ref('mart__fact_all_taxi_trips') }}
),
locations AS (
    SELECT
        LocationID,
        Zone,
        Borough
    FROM
        {{ ref('mart__dim_locations') }}
)
-- this querys filters the trips with unknown pickup location_id
SELECT
    Borough,
    Zone, 
    init_location,
    final_location,
    pickup_datetime,
    dropOff_datetime,
    duration_min
    
FROM 
    trips t
JOIN
    locations b_init ON t.init_location = b_init.LocationID
WHERE init_location = 264 OR init_location = 265 AND Borough = 'Unknown'   
LIMIT 4000

