--- Author: dgarciab

-- this query finds all the Zones where there are less than 100,000 trips.

--->functions (CTE) with the trips table and locations. 
 WITH trips AS (
    SELECT
        type,
        duration_min,
        pulocationid AS init_location,
        dolocationid AS final_location
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
--> query to calculate the number of trips and average duration by borough and zone
--> HAVING this condition:  total_trips < 100000
SELECT
    Borough,
    Zone, 
    COUNT(*) AS total_trips,
    AVG(duration_min) AS average_duration_min

FROM 
    trips t
JOIN
    locations b_init ON t.init_location = b_init.LocationID
   
GROUP BY
    Borough,
    Zone
HAVING total_trips < 100000
ORDER BY ALL




