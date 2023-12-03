
---> Author: dgarciab
---> Query description: 

--Find the average time between taxi pick ups per zone
--Use lead or lag to find the next trip per zone for each record
--then find the time difference between the pick up time for the current record
--and the next
--then use this result to calculate the average time between pick ups per zone.

--->functions (CTE) with the  taxi trips table and locations. 
 WITH trips AS (
    SELECT
        pickup_datetime,
        pulocationid AS init_location
    FROM
        {{ ref('mart__fact_all_taxi_trips') }}
),
locations AS (
    SELECT
        LocationID,
        Zone
    FROM
        {{ ref('mart__dim_locations') }}
),

-- find the next trip picktime using lead per zone for each record
zonePickupTime AS ( SELECT
    pickup_datetime,
    Zone,
    (LEAD(pickup_datetime) OVER (PARTITION BY Zone ORDER BY pickup_datetime)) AS next_pickup_datetime
FROM 
    trips t
JOIN
    locations b_init ON t.init_location = b_init.LocationID
)
-- this query calculate the average time difference between the pick up time for the current record
--and the next
SELECT
    Zone,
    AVG(datediff('minute',pickup_datetime, next_pickup_datetime)) as avg_time_b_pickups_min
FROM zonePickupTime
Group by Zone; 

 
    
