--author dgarciab
-- this query calculates the 7 day moving min, max, avg, sum for precipitation
--and snow for every day in the weather data, defining the window only once

SELECT  
    date, 
    prcp,
    snow,
    AVG(prcp) OVER seven_days AS avg_precipitation7,
    MIN(prcp) OVER seven_days AS min_precipitation7,
    MAX(prcp) OVER seven_days AS max_precipitation7,
    SUM(prcp) OVER seven_days AS sum_precipitation7,
    AVG(snow) OVER seven_days AS avg_snow7,
    MIN(snow) OVER seven_days AS min_snow7,
    MAX(snow) OVER seven_days AS max_snow7,
    SUM(snow) OVER seven_days AS sum_snow7
FROM 
   {{ ref('stg__central_park_weather') }}
    -- windos definition, to be reused in all the aggregations
WINDOW seven_days AS (ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)
ORDER BY
    date;