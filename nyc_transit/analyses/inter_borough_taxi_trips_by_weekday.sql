WITH trips AS (
    SELECT
        type,
        weekday(pickup_datetime) AS weekday,
        pulocationid AS init_location,
        dolocationid AS final_location
    FROM
        {{ ref('mart__fact_all_taxi_trips') }}
),
locations AS (
    SELECT
        LocationID,
        Borough
    FROM
        {{ ref('mart__dim_locations') }}
)

SELECT
    weekday,
    COUNT(*) AS total_trips,
    SUM(CASE WHEN init_location != final_location AND b_init.Borough != b_final.Borough  THEN 1 ELSE 0 END) AS Total_trips_dif_boroughs,
    100.0 * SUM(CASE WHEN init_location != final_location AND b_init.Borough != b_final.Borough THEN 1 ELSE 0 END) / COUNT(*) AS Percentage_diff_boroughs


FROM
    trips t
JOIN
    locations b_init ON t.init_location = b_init.LocationID
JOIN
    locations b_final ON t.final_location = b_final.LocationID
GROUP BY
    weekday;
