-- author dgarciab
--  this  query compare an individual fare to the zone, borough and overall average fare using the fare_amount in yellow taxi trip data

--->functions (CTE) with the trips table and locations. 
 WITH trips AS (
    SELECT
    
        fare_amount, 
        pulocationid AS init_location
    FROM
        {{ ref('stg__yellow_tripdata') }}
),
locations AS (
    SELECT
        LocationID,
        Zone,
        Borough
    FROM
        {{ ref('mart__dim_locations') }}
),
--> join locations and trips tables, including the columns needed for the analysis
tripsByborough AS (
  SELECT
    Borough,
    Zone,
    init_location, 
    fare_amount, 
FROM 
    trips t
JOIN
    locations b_init ON t.init_location = b_init.LocationID
)
---> query to compare an individual fare to the zone, borough and overall average fare
SELECT
    *,
    AVG(fare_amount) OVER () AS overall_avg_fare,
    AVG(fare_amount) OVER (PARTITION BY Borough) AS borough_avg_fare,
    AVG(fare_amount) OVER (PARTITION BY Zone) AS Zone_avg_fare
  
FROM
  tripsByborough
---> limit the result to extract just a sample of the answer, since the final file size is quite big to upload it to github
LIMIT 2000

    
