---> Author: dgarciab
--> This query Calculate the number of trips by borough

--->First we create a CTE with the trips table
WITH trips AS (
    SELECT
        type,
        pulocationid AS init_location
    FROM
        {{ ref('mart__fact_all_taxi_trips') }}
),
--->Then we create a CTE with the locations table
locations AS (
    SELECT
        LocationID,
        Borough
    FROM
        {{ ref('mart__dim_locations') }}
)

SELECT
    Borough,
    COUNT(*) AS total_trips_by_borough
FROM
    trips t
JOIN
    locations b_init ON t.init_location = b_init.LocationID
GROUP BY
    Borough
