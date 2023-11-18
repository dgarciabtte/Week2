
SELECT
    COUNT(*) AS total_trips
FROM
    {{ ref('mart__fact_all_taxi_trips') }} trips
JOIN
    {{ ref('mart__dim_locations') }} loc
ON
    trips.dolocationid = loc.LocationID
WHERE
    loc.service_zone = 'Airports' OR loc.service_zone = 'EWR'






   
   