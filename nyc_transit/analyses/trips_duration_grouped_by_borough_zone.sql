---> Author: dgarciab
---> Description: This query calculates the number of trips and average duration by borough and zone
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
SELECT
    Borough,
    Zone, 
    COUNT(*) AS total_trips,
    ROUND(AVG(duration_min), 2) AS average_duration_min

FROM 
    trips t
JOIN
    locations b_init ON t.init_location = b_init.LocationID
   
GROUP BY
    Borough,
    Zone
ORDER BY ALL

