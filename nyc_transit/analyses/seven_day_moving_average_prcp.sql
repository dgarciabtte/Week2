--   this query  calculates the 7 day moving average precipitation for every day in
--the weather data

SELECT  
     date, 
     prcp,
     -- calculate the average over 7 days (3 days before, the current day, and 3 days after), the query should include the precipitation (prcp) from each of these days.
     (LAG(prcp, 3) OVER (ORDER BY date) + LAG(prcp, 2) OVER (ORDER BY date) + LAG(prcp, 1) OVER (ORDER BY date) + prcp  + LEAD(prcp, 3) OVER (ORDER BY date)+ LEAD(prcp, 2) OVER (ORDER BY date)+ LEAD(prcp, 1) OVER (ORDER BY date)) / 7 AS avg_precipitation7

FROM {{ ref('stg__central_park_weather') }}
ORDER BY
    date;

